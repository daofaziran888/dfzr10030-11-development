"""通道接入批次包领域逻辑：分片合并、背压切批、版本并发控制与共享快照。

边界说明（跟着接入通道服务取数逻辑定位的格式解析服务边界）：
- 本模块只放批次包的构建/读取规则，不挂任何路由；
- 接入通道服务（ingestion）负责登记分片、触发构建，是批次包的写入方；
- 格式解析服务（parser）只读同一份批次包、回写待审核工作台项与编排指令；
- 两边通过 app.store 里的同一张 ingestion_batch 表共享数据，不存在两份副本。

两条硬约束（任何合并策略都必须守住）：
1. 解析端只能读取“背压完整”的批次：单分片超过通道背压阈值的分片永远不得进入完成批次；
2. 每个 leaf 批次通道可追溯、输入格式单一，避免解析端收到无法解释的混合数据。
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import datetime
from typing import Any

from app.store import store

# ---- 内存表名（store 里与业务模块表并列，但不进运营概览） ----
SHARD_MODULE = "ingestion_shard"
BATCH_MODULE = "ingestion_batch"
REVIEW_MODULE = "review_workbench"
BUILD_MODULE = "batch_build_record"
INTERNAL_MODULES = {SHARD_MODULE, BATCH_MODULE, REVIEW_MODULE, BUILD_MODULE}

# ---- 合并策略 ----
CHANNEL_FIRST = "channel_first"   # 按通道合并分片并在阈值处切开：背压边界前置决定切断位置
FORMAT_FIRST = "format_first"     # 按输入格式分别成批再汇总通道批次：输入格式前置决定子批次结构
STRATEGIES = (CHANNEL_FIRST, FORMAT_FIRST)
STRATEGY_LABELS = {
    CHANNEL_FIRST: "按通道合并（背压边界前置）",
    FORMAT_FIRST: "按格式成批（输入格式前置）",
}

# ---- 分片/批次/审核状态 ----
SHARD_PENDING = "待入批"
SHARD_BATCHED = "已入批"
SHARD_BLOCKED = "超限拦截"
BATCH_COMPLETE = "背压完整"
LEVEL_LEAF = "leaf"
LEVEL_CHANNEL = "channel"
LEVEL_GROUP = "group"
REVIEW_PENDING = "待审核"
REVIEW_PASSED = "已通过"
REVIEW_REJECTED = "已驳回"

# 同一版本并发提交需要串行化；内存仓库下进程内锁即可保证“仅一次成功”。
_build_lock = threading.RLock()


# ============================ 读取辅助 ============================

def list_shards(channel_code: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows(SHARD_MODULE)
    if channel_code:
        rows = [r for r in rows if r.get("通道编号") == channel_code]
    if status:
        rows = [r for r in rows if r.get("分片状态") == status]
    return sorted(rows, key=lambda r: (str(r.get("通道编号")), int(r.get("队列序号", 0)), int(r.get("id", 0))))


def list_batches(
    *,
    level: str | None = None,
    channel_code: str | None = None,
    input_format: str | None = None,
) -> list[dict[str, Any]]:
    rows = store.rows(BATCH_MODULE)
    if level:
        rows = [r for r in rows if r.get("批次层级") == level]
    if channel_code:
        rows = [r for r in rows if channel_code in (_channels_of(r))]
    if input_format:
        rows = [r for r in rows if input_format in str(r.get("输入格式", "")).split("、")]
    return sorted(rows, key=lambda r: int(r.get("id", 0)))


def get_batch_by_no(batch_no: str) -> dict[str, Any] | None:
    for row in store.rows(BATCH_MODULE):
        if row.get("批次编号") == batch_no:
            return row
    return None


def get_batch_detail(batch_no: str) -> dict[str, Any] | None:
    """批次明细：把分片编号 hydration 成完整分片，便于页面核对分片来源与背压切断点。"""
    batch = get_batch_by_no(batch_no)
    if batch is None:
        return None
    shard_index = {s.get("分片编号"): s for s in store.rows(SHARD_MODULE)}
    detail = dict(batch)
    detail["分片明细"] = [shard_index[no] for no in batch.get("分片编号列表", []) if no in shard_index]
    if batch.get("批次层级") == LEVEL_LEAF:
        review = next((r for r in store.rows(REVIEW_MODULE) if r.get("批次编号") == batch_no), None)
        detail["工作台项"] = review
        detail["建议解析配置"] = _resolve_parser(str(batch.get("输入格式", "")))
    return detail


def list_review_items(status: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows(REVIEW_MODULE)
    if status:
        rows = [r for r in rows if r.get("审核状态") == status]
    return sorted(rows, key=lambda r: int(r.get("id", 0)))


def list_build_records() -> list[dict[str, Any]]:
    return sorted(store.rows(BUILD_MODULE), key=lambda r: int(r.get("id", 0)))


def _channels_of(batch_row: dict[str, Any]) -> list[str]:
    if batch_row.get("批次层级") == LEVEL_GROUP:
        return list(batch_row.get("通道编号列表", []))
    return [str(batch_row.get("通道编号", ""))]


def _resolve_parser(input_format: str) -> dict[str, Any] | None:
    """格式解析服务按输入格式选择字段映射：优先生效中，其次同格式任一解析配置。"""
    parsers = store.rows("parser")
    matched = [p for p in parsers if p.get("输入格式") == input_format]
    return next((p for p in matched if p.get("status") == "生效中"), None) or (matched[0] if matched else None)


# ============================ 分片登记 ============================

def register_shard(values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    """接入通道侧登记输入分片；分片必须绑定通道与输入格式，背压阈值取自通道快照。"""
    required = ["通道编号", "输入格式", "分片大小"]
    missing = [f for f in required if not str(values.get(f) or "").strip()]
    if missing:
        return None, missing
    channel = _find_channel(str(values["通道编号"]))
    if channel is None:
        return None, [f"通道 {values['通道编号']} 不存在，分片无法绑定通道"]
    try:
        size = int(values["分片大小"])
        if size <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return None, ["分片大小必须为正整数"]
    threshold = int(channel.get("背压阈值"))
    rows = store.rows(SHARD_MODULE)
    same_channel = [r for r in rows if r.get("通道编号") == channel["通道编号"]]
    seq = max((int(r.get("队列序号", 0)) for r in same_channel), default=0) + 1
    entry = {
        "id": _next_id(SHARD_MODULE),
        "分片编号": f"SHARD-{int(channel['id']):02d}{seq:02d}",
        "通道编号": channel["通道编号"],
        "输入格式": str(values["输入格式"]).strip(),
        "队列序号": seq,
        "分片大小": size,
        "背压阈值": threshold,
        "入队时间": datetime.now().isoformat(timespec="seconds"),
        "分片状态": SHARD_BLOCKED if size > threshold else SHARD_PENDING,
    }
    rows.append(entry)
    return entry, []


def _find_channel(channel_code: str) -> dict[str, Any] | None:
    return next((r for r in store.rows("ingestion") if r.get("通道编号") == channel_code), None)


# ============================ 批次构建（两种边界的取舍） ============================

def preview_batches(channel_codes: list[str], strategy: str) -> dict[str, Any]:
    """不落库的切批预演：供页面对比两种边界，确认“通道接入批次包结果不同”后再正式提交。"""
    if strategy not in STRATEGIES:
        raise ValueError(f"未知合并策略：{strategy}")
    channels = [c for c in channel_codes if _find_channel(c)]
    projection: list[dict[str, Any]] = []
    for code in channels:
        threshold, pending, blocked = _channel_pending(code)
        leaves = _split(pending, threshold, strategy)
        projection.append({
            "通道编号": code,
            "背压阈值": threshold,
            "待入批分片数": len(pending),
            "超限拦截分片": [s["分片编号"] for s in blocked],
            "预演leaf批次": [
                {"输入格式": shards[0]["输入格式"], "分片编号列表": [s["分片编号"] for s in shards],
                 "批次大小": sum(int(s["分片大小"]) for s in shards)}
                for shards in leaves
            ],
        })
    return {"合并策略": strategy, "策略说明": STRATEGY_LABELS[strategy], "通道预演": projection}


def build_batches(
    channel_codes: list[str],
    strategy: str,
    *,
    data_versions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """多选接入通道整组写入批次包；同一把锁内串行处理，逐台记录成功/失败。

    历史不变：只消费“待入批”分片，已入批分片与历史批次永不参与重算；
    背压控制直接且批次数量较少：超阈值分片直接拦截在完成批次之外。
    """
    if strategy not in STRATEGIES:
        return {"ok": False, "message": f"未知合并策略：{strategy}", "results": []}
    data_versions = data_versions or {}
    with _build_lock:
        now = datetime.now().isoformat(timespec="seconds")
        results: list[dict[str, Any]] = []
        group_leaf_nos: list[str] = []
        group_channels: list[str] = []
        channel_versions: dict[str, str] = {}
        created_batch_nos: list[str] = []

        for code in channel_codes:
            channel = _find_channel(code)
            if channel is None:
                results.append({"通道编号": code, "ok": False, "message": "接入通道不存在，整台跳过"})
                continue
            # 同一版本同时提交仅一次成功：显式版本号已落库则直接拦住。
            submitted_version = data_versions.get(code)
            if submitted_version and _version_exists(code, submitted_version):
                results.append({
                    "通道编号": code, "ok": False, "message": f"数据版本 {submitted_version} 已成功提交过，重复提交已拦截",
                })
                continue

            threshold, pending, blocked = _channel_pending(code)
            # 超阈值分片前置拦截并落状态，绝不允许进入完成批次。
            for shard in blocked:
                shard["分片状态"] = SHARD_BLOCKED
            if not pending:
                results.append({
                    "通道编号": code, "ok": False,
                    "message": "没有背压完整的待入批分片（超阈值分片已拦截），本台未生成批次",
                    "超限拦截分片": [s["分片编号"] for s in blocked],
                })
                _refresh_channel_queue(channel)
                continue

            leaves_spec = _split(pending, threshold, strategy)
            version = submitted_version or _next_version(code)
            if _version_exists(code, version):
                results.append({"通道编号": code, "ok": False,
                                "message": f"数据版本 {version} 已存在，同一版本仅允许提交成功一次"})
                continue

            leaf_nos: list[str] = []
            leaf_specs: list[list[dict[str, Any]]] = []
            for shards in leaves_spec:
                batch_no = _next_batch_no()
                leaf = _build_leaf_row(batch_no, channel, shards, threshold, strategy, version, now)
                store.rows(BATCH_MODULE).append(leaf)
                leaf_nos.append(batch_no)
                leaf_specs.append(shards)
                for shard in shards:
                    shard["分片状态"] = SHARD_BATCHED
                    shard["所属批次"] = batch_no
                store.rows(REVIEW_MODULE).append(_build_review_row(leaf, channel, now))

            channel_rollup_no = _next_batch_no()
            store.rows(BATCH_MODULE).append(
                _build_channel_rollup(channel_rollup_no, channel, leaf_nos, leaf_specs,
                                      threshold, strategy, version, now))
            created_batch_nos.extend(leaf_nos)
            created_batch_nos.append(channel_rollup_no)
            channel_versions[code] = version
            _refresh_channel_queue(channel)
            group_leaf_nos.extend(leaf_nos)
            group_channels.append(code)
            results.append({
                "通道编号": code, "ok": True,
                "message": f"{STRATEGY_LABELS[strategy]}：生成 {len(leaf_nos)} 个背压完整 leaf 批次并汇总通道批次",
                "数据版本": version,
                "leaf批次": leaf_nos, "通道批次": channel_rollup_no,
                "超限拦截分片": [s["分片编号"] for s in blocked],
            })

        group_no = None
        if len(channel_codes) > 1 and group_leaf_nos:
            group_no = _next_batch_no()
            store.rows(BATCH_MODULE).append(_build_group_rollup(
                group_no, group_channels, group_leaf_nos, strategy, now, channel_versions))

        build_no = _next_build_no()
        record = {
            "id": _next_id(BUILD_MODULE),
            "构建记录编号": build_no,
            "合并策略": strategy,
            "策略说明": STRATEGY_LABELS[strategy],
            "通道编号列表": channel_codes,
            "组批次编号": group_no,
            "成功通道": [r["通道编号"] for r in results if r["ok"]],
            "失败通道": [r["通道编号"] for r in results if not r["ok"]],
            "结果明细": results,
            "构建时间": now,
        }
        store.rows(BUILD_MODULE).append(record)
        for batch_no in created_batch_nos + ([group_no] if group_no else []):
            row = get_batch_by_no(batch_no)
            if row is not None:
                row["构建记录编号"] = build_no
        return {"ok": True, "构建记录编号": build_no, "组批次编号": group_no,
                "合并策略": strategy, "results": results}


def _channel_pending(code: str) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
    channel = _find_channel(code)
    threshold = int(channel.get("背压阈值"))
    shards = sorted(
        (s for s in store.rows(SHARD_MODULE) if s.get("通道编号") == code),
        key=lambda s: (int(s.get("队列序号", 0)), int(s.get("id", 0))),
    )
    blocked = [s for s in shards if int(s["分片大小"]) > threshold]
    pending = [s for s in shards if s.get("分片状态") == SHARD_PENDING and int(s["分片大小"]) <= threshold]
    return threshold, pending, blocked


def _split(pending: list[dict[str, Any]], threshold: int, strategy: str) -> list[list[dict[str, Any]]]:
    if strategy == CHANNEL_FIRST:
        return _split_channel_first(pending, threshold)
    return _split_format_first(pending, threshold)


def _split_channel_first(pending: list[dict[str, Any]], threshold: int) -> list[list[dict[str, Any]]]:
    """背压边界前置：按通道队列顺序消费，遇格式切换或超阈值即切，阈值决定切断位置。"""
    leaves: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_size = 0
    current_format: str | None = None
    for shard in pending:
        size, fmt = int(shard["分片大小"]), str(shard["输入格式"])
        if current and (fmt != current_format or current_size + size > threshold):
            leaves.append(current)
            current, current_size, current_format = [], 0, fmt
        if not current:
            current_format = fmt
        current.append(shard)
        current_size += size
    if current:
        leaves.append(current)
    return leaves


def _split_format_first(pending: list[dict[str, Any]], threshold: int) -> list[list[dict[str, Any]]]:
    """输入格式前置：先按格式拆成子队列（保持首次出现顺序），各子队列再按背压阈值成批。"""
    grouped: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
    for shard in pending:
        grouped.setdefault(str(shard["输入格式"]), []).append(shard)
    leaves: list[list[dict[str, Any]]] = []
    for fmt, items in grouped.items():
        current: list[dict[str, Any]] = []
        current_size = 0
        for shard in items:
            size = int(shard["分片大小"])
            if current and current_size + size > threshold:
                leaves.append(current)
                current, current_size = [], 0
            current.append(shard)
            current_size += size
        if current:
            leaves.append(current)
    return leaves


# ============================ 批次行组装与快照 ============================

def _build_leaf_row(batch_no: str, channel: dict[str, Any], shards: list[dict[str, Any]],
                    threshold: int, strategy: str, version: str, now: str) -> dict[str, Any]:
    batch_size = sum(int(s["分片大小"]) for s in shards)
    formats = {str(s["输入格式"]) for s in shards}
    assert len(formats) == 1 and batch_size <= threshold, "leaf 必须格式单一且背压完整"
    return {
        "id": _next_id(BATCH_MODULE),
        "批次编号": batch_no,
        "批次层级": LEVEL_LEAF,
        "父批次编号": None,  # 持久化后由通道汇总批次回填
        "通道编号": channel["通道编号"],
        "输入格式": next(iter(formats)),
        "合并策略": strategy,
        "批次状态": BATCH_COMPLETE,
        "审核状态": REVIEW_PENDING,
        "分片编号列表": [s["分片编号"] for s in shards],
        "分片数量": len(shards),
        "批次大小": batch_size,
        "背压阈值": threshold,
        "切断点说明": f"队列累计 {batch_size}/{threshold} 达到背压边界，批次在此闭合（{STRATEGY_LABELS[strategy]}）",
        "通道快照": _channel_snapshot(channel),
        "背压快照": {"队列水位": batch_size, "背压阈值": threshold, "是否超限": False,
                  "切断点队列累计": batch_size},
        "数据版本": version,
        "构建记录编号": None,
        "创建时间": now,
    }


def _build_channel_rollup(rollup_no: str, channel: dict[str, Any], leaf_nos: list[str],
                          leaf_specs: list[list[dict[str, Any]]], threshold: int,
                          strategy: str, version: str, now: str) -> dict[str, Any]:
    all_shards = [s for group in leaf_specs for s in group]
    batch_size = sum(int(s["分片大小"]) for s in all_shards)
    formats = "、".join(dict.fromkeys(str(s["输入格式"]) for s in all_shards))
    rollup = {
        "id": _next_id(BATCH_MODULE),
        "批次编号": rollup_no,
        "批次层级": LEVEL_CHANNEL,
        "父批次编号": None,
        "通道编号": channel["通道编号"],
        "输入格式": formats,
        "合并策略": strategy,
        "批次状态": BATCH_COMPLETE,
        "审核状态": REVIEW_PENDING,
        "分片编号列表": [s["分片编号"] for s in all_shards],
        "分片数量": len(all_shards),
        "批次大小": batch_size,
        "背压阈值": threshold,
        "切断点说明": "；".join(
            f"{no} 在 {sum(int(x['分片大小']) for x in group)}/{threshold} 处切断"
            for no, group in zip(leaf_nos, leaf_specs)),
        "通道快照": _channel_snapshot(channel),
        "背压快照": {"队列水位": batch_size, "背压阈值": threshold, "是否超限": False,
                  "leaf批次数": len(leaf_nos)},
        "数据版本": version,
        "构建记录编号": None,
        "子批次编号列表": leaf_nos,
        "创建时间": now,
    }
    for no in leaf_nos:
        leaf = get_batch_by_no(no)
        if leaf:
            leaf["父批次编号"] = rollup_no
    return rollup


def _build_group_rollup(group_no: str, channel_codes: list[str], leaf_nos: list[str],
                        strategy: str, now: str, channel_versions: dict[str, str]) -> dict[str, Any]:
    leaf_rows = [get_batch_by_no(no) for no in leaf_nos]
    leaf_rows = [r for r in leaf_rows if r]
    all_shard_nos = [no for r in leaf_rows for no in r.get("分片编号列表", [])]
    formats = "、".join(dict.fromkeys(str(r.get("输入格式", "")) for r in leaf_rows))
    version_text = "、".join(f"{c}@{channel_versions.get(c, '?')}" for c in channel_codes)
    return {
        "id": _next_id(BATCH_MODULE),
        "批次编号": group_no,
        "批次层级": LEVEL_GROUP,
        "父批次编号": None,
        "通道编号": "、".join(channel_codes),
        "通道编号列表": channel_codes,
        "输入格式": formats,
        "合并策略": strategy,
        "批次状态": BATCH_COMPLETE,
        "审核状态": REVIEW_PENDING,
        "分片编号列表": all_shard_nos,
        "分片数量": len(all_shard_nos),
        "批次大小": sum(int(r["批次大小"]) for r in leaf_rows),
        "背压阈值": None,
        "切断点说明": f"组批次汇总 {len(channel_codes)} 个通道、{len(leaf_nos)} 个 leaf 批次",
        "通道快照": {"通道编号列表": channel_codes},
        "背压快照": {"是否超限": False, "leaf批次数": len(leaf_nos)},
        "数据版本": version_text,
        "构建记录编号": None,
        "子批次编号列表": leaf_nos,
        "创建时间": now,
    }


def _build_review_row(leaf: dict[str, Any], channel: dict[str, Any], now: str) -> dict[str, Any]:
    parser = _resolve_parser(str(leaf["输入格式"]))
    review_id = _next_id(REVIEW_MODULE)
    return {
        "id": review_id,
        "工作台项编号": f"REV-{review_id:04d}",
        "批次编号": leaf["批次编号"],
        "通道编号": channel["通道编号"],
        "输入格式": leaf["输入格式"],
        "审核状态": REVIEW_PENDING,
        "解析编号": parser.get("解析编号") if parser else None,
        "字段映射摘要": parser.get("字段映射") if parser else "暂无匹配解析配置",
        "编排指令": "",
        "创建时间": now,
        "处理时间": None,
    }


def _channel_snapshot(channel: dict[str, Any]) -> dict[str, Any]:
    return {key: channel.get(key) for key in
            ("通道编号", "通道名称", "输入格式", "解析引擎", "背压阈值", "缓冲队列", "通道状态")}


def _refresh_channel_queue(channel: dict[str, Any]) -> None:
    """同一动作同步更新接入通道服务：缓冲队列统计仍滞留（待入批/超限拦截）的分片。

    滞留量保留超限分片，页面才能看到“队列接近阈值且存在无法解释的分片”的背压状态。
    """
    remain = [s for s in store.rows(SHARD_MODULE)
              if s.get("通道编号") == channel["通道编号"] and s.get("分片状态") != SHARD_BATCHED]
    channel["缓冲队列"] = sum(int(s["分片大小"]) for s in remain)
    channel["abnormal"] = any(s.get("分片状态") == SHARD_BLOCKED for s in remain)


# ============================ 解析侧审核与编排 ============================

def review_leaf(batch_no: str, action: str, remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
    leaf = get_batch_by_no(batch_no)
    if leaf is None or leaf.get("批次层级") != LEVEL_LEAF:
        return None, "只能对 leaf 通道接入批次包执行审核"
    review = next((r for r in store.rows(REVIEW_MODULE) if r.get("批次编号") == batch_no), None)
    if review is None:
        return None, "该批次没有待审核工作台项"
    if review["审核状态"] != REVIEW_PENDING:
        return None, f"工作台项已为「{review['审核状态']}」，不能重复审核"
    if action not in (REVIEW_PASSED, REVIEW_REJECTED):
        return None, "审核动作只支持 通过 / 驳回"
    review["审核状态"] = action
    review["处理时间"] = datetime.now().isoformat(timespec="seconds")
    if remark:
        review["审核备注"] = remark
    leaf["审核状态"] = action
    return review, f"批次 {batch_no} 审核结果：{action}"


def orchestrate(batch_no: str, instruction: str | None = None) -> tuple[dict[str, Any] | None, str]:
    """格式解析服务按批次包编排接入通道后续处理；审核通过的批次才能编排。"""
    leaf = get_batch_by_no(batch_no)
    if leaf is None or leaf.get("批次层级") != LEVEL_LEAF:
        return None, "只能对 leaf 通道接入批次包编排后续处理"
    review = next((r for r in store.rows(REVIEW_MODULE) if r.get("批次编号") == batch_no), None)
    if review is None:
        return None, "该批次缺少工作台项，无法编排"
    if review["审核状态"] != REVIEW_PASSED:
        return None, "批次尚未审核通过，解析端不能读取编排"
    if review.get("编排指令"):
        return None, "该批次已编排，重复编排已拦截"
    parser = _resolve_parser(str(leaf["输入格式"]))
    text = instruction or (
        f"按 {parser.get('解析编号')}（{leaf['输入格式']} 字段映射）编排通道 {leaf['通道编号']} 的后续处理"
        if parser else f"按批次 {batch_no} 的输入格式编排后续处理")
    review["编排指令"] = text
    review["编排时间"] = datetime.now().isoformat(timespec="seconds")
    return review, f"批次 {batch_no} 后续处理已编排"


# ============================ 编号与版本 ============================

def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _next_batch_no() -> str:
    return f"CIB-{_next_id(BATCH_MODULE):04d}"


def _next_build_no() -> str:
    return f"BLD-{datetime.now().strftime('%Y%m%d')}-{_next_id(BUILD_MODULE):04d}"


def _next_version(channel_code: str) -> str:
    """取该通道未占用的最小版本号；历史版本（如种子批次 v1）不可被复用。"""
    used = {r.get("数据版本") for r in store.rows(BATCH_MODULE) if r.get("通道编号") == channel_code}
    index = 1
    while f"{channel_code}@v{index}" in used:
        index += 1
    return f"{channel_code}@v{index}"


def _version_exists(channel_code: str, version: str) -> bool:
    return any(r.get("数据版本") == version and r.get("通道编号") == channel_code
               for r in store.rows(BATCH_MODULE))
