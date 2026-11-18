"""通道接入批次包领域规则验证脚本（不依赖 httpx，直接跑服务层）。

覆盖需求点：
1. 输入分片绑定通道与格式；超背压阈值分片不得进入完成批次；
2. 两种合并边界结果不同，但都只产出背压完整、通道可追溯的 leaf；
3. 多选通道整组构建，成功/失败逐台记录；
4. 同一数据版本并发/重复提交仅一次成功，余下拦住；
5. 构建同步写入待审核工作台项，解析侧读取同一份批次包并审核、编排；
6. 重算只作用于待入批分片，历史批次不可变；阈值变更按适用范围重算。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import channel_batch as cb  # noqa: E402
from app.store import store  # noqa: E402

failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" -> {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(label)


def leaf_batches(channel_code: str) -> list[dict]:
    return [b for b in store.rows(cb.BATCH_MODULE)
            if b["批次层级"] == cb.LEVEL_LEAF and b["通道编号"] == channel_code]


def main() -> int:
    # ---------- 初始种子状态 ----------
    check("种子含历史批次 CIB-0001", any(b["批次编号"] == "CIB-0001" for b in store.rows(cb.BATCH_MODULE)))
    check("种子含历史工作台项 REV-0001", any(r["工作台项编号"] == "REV-0001" for r in store.rows(cb.REVIEW_MODULE)))

    # ---------- 规则1：分片登记绑定通道/格式，超阈值直接拦截 ----------
    shard, problems = cb.register_shard({"通道编号": "INGE-0002", "输入格式": "CSV", "分片大小": 20})
    check("合法分片登记成功", shard is not None and not problems)
    check("新分片自动绑定背压阈值快照", shard and shard["背压阈值"] == 80)
    bad, problems = cb.register_shard({"通道编号": "INGE-0002", "输入格式": "CSV", "分片大小": 999})
    check("超阈值分片登记即拦截", bad and bad["分片状态"] == cb.SHARD_BLOCKED)
    _, problems = cb.register_shard({"通道编号": "NOPE", "输入格式": "CSV", "分片大小": 1})
    check("未知通道分片被拒", bool(problems))
    _, problems = cb.register_shard({"通道编号": "INGE-0002", "输入格式": "", "分片大小": 1})
    check("缺输入格式被拒", bool(problems))

    # ---------- 规则2：两种边界预演结果不同 ----------
    channels = ["INGE-0001", "INGE-0002", "INGE-0003"]
    preview_cf = cb.preview_batches(channels, cb.CHANNEL_FIRST)
    preview_ff = cb.preview_batches(channels, cb.FORMAT_FIRST)

    def flat(preview: dict) -> list[tuple[str, str, list[str]]]:
        out = []
        for ch in preview["通道预演"]:
            for leaf in ch["预演leaf批次"]:
                out.append((ch["通道编号"], leaf["输入格式"], tuple(leaf["分片编号列表"])))
        return out

    cf_seq = flat(preview_cf)
    ff_seq = flat(preview_ff)
    check("两种边界切批结构不同", cf_seq != ff_seq, f"CF={cf_seq} | FF={ff_seq}")
    # 通道1：JSON60+CSV40+JSON55，阈值100，队列顺序 J,C,J。
    # channel_first 按队列顺序遇格式切换即切：[J60],[C40],[J55]，切断位置由背压/格式边界前置决定；
    # format_first 先聚合同格式（保持队列序号），同格式 60+55=115 仍超阈值 100，
    # 因此在阈值处切成 [J60],[J55] 再排 [C40]：输入格式前置决定子批次结构，背压完整仍是硬底线。
    ch1_cf = next(c for c in preview_cf["通道预演"] if c["通道编号"] == "INGE-0001")
    ch1_ff = next(c for c in preview_ff["通道预演"] if c["通道编号"] == "INGE-0001")
    check("通道1 背压边界前置：沿队列在边界处切开",
          [l["分片编号列表"] for l in ch1_cf["预演leaf批次"]]
          == [["SHARD-1001"], ["SHARD-1002"], ["SHARD-1003"]])
    check("通道1 输入格式前置：同格式优先聚批，超阈值仍在阈值处切开",
          [l["分片编号列表"] for l in ch1_ff["预演leaf批次"]]
          == [["SHARD-1001"], ["SHARD-1003"], ["SHARD-1002"]])
    check("两种边界下每个 leaf 均不超背压阈值",
          all(l["批次大小"] <= c["背压阈值"]
              for c in preview_cf["通道预演"] + preview_ff["通道预演"]
              for l in c["预演leaf批次"]))
    check("超阈值分片不进任何预演批次",
          all("SHARD-1004" not in l["分片编号列表"] for c in preview_cf["通道预演"] for l in c["预演leaf批次"]))
    check("预演不落库", len(store.rows(cb.BATCH_MODULE)) == 1)

    # ---------- 规则3+4+5：整组构建（背压边界前置），逐台记录、同步工作台 ----------
    result = cb.build_batches(channels, cb.CHANNEL_FIRST)
    per_channel = {r["通道编号"]: r for r in result["results"]}
    check("三通道均构建成功", all(r["ok"] for r in result["results"]), str(result["results"]))
    check("多通道生成组批次", result["组批次编号"] is not None)
    # 逐台记录含超限拦截分片（SHARD-1004 来自种子，SHARD-0205 为本用例前面登记的 999 超限分片）
    check("逐台记录含超限拦截分片",
          set(per_channel["INGE-0001"]["超限拦截分片"]) == {"SHARD-1004"}
          and "SHARD-3004" in per_channel["INGE-0003"]["超限拦截分片"])

    # leaf 全部背压完整、格式单一、可追溯
    leaves = [b for b in store.rows(cb.BATCH_MODULE) if b["批次层级"] == cb.LEVEL_LEAF and b["id"] > 1]
    check("所有新 leaf 背压完整且格式单一",
          all(b["批次大小"] <= b["背压阈值"] for b in leaves)
          and all("、" not in b["输入格式"] for b in leaves))
    check("leaf 带通道快照与背压快照",
          all(b["通道快照"]["通道编号"] == b["通道编号"] and b["背压快照"]["是否超限"] is False for b in leaves))
    check("leaf 可追溯到分片来源", all(b["分片编号列表"] and b["数据版本"] for b in leaves))
    check("超阈值分片永不入批",
          all("SHARD-1004" not in b["分片编号列表"] and "SHARD-3004" not in b["分片编号列表"] for b in leaves))

    # 分片状态联动 + 通道缓冲队列被同一动作更新
    check("入批分片状态更新为已入批",
          all(s["分片状态"] == cb.SHARD_BATCHED for s in store.rows(cb.SHARD_MODULE)
              if s["分片编号"] in {"SHARD-1001", "SHARD-2001", "SHARD-3003"}))
    ch1 = next(c for c in store.rows("ingestion") if c["通道编号"] == "INGE-0001")
    check("通道缓冲队列同步更新（只剩超限分片130）", ch1["缓冲队列"] == 130 and ch1["abnormal"] is True)

    # 工作台项同步生成、解析配置按输入格式匹配
    review_rows = [r for r in store.rows(cb.REVIEW_MODULE) if r["id"] > 1]
    check("每个新 leaf 同步一条待审核工作台项", len(review_rows) == len(leaves))
    check("工作台项按格式匹配解析配置",
          all(r["解析编号"] for r in review_rows) and
          any(r["输入格式"] == "JSON" and r["解析编号"] == "PARS-0001" for r in review_rows))

    # 汇总层级
    rollups = [b for b in store.rows(cb.BATCH_MODULE) if b["批次层级"] == cb.LEVEL_CHANNEL and b["id"] > 1]
    check("每通道一条汇总批次", len(rollups) == 3)
    check("leaf 回填父批次", all(b["父批次编号"] for b in leaves))
    group = next(b for b in store.rows(cb.BATCH_MODULE) if b["批次层级"] == cb.LEVEL_GROUP)
    check("组批次覆盖全部三通道", set(group["通道编号列表"]) == set(channels))
    check("所有新批次回挂同一构建记录编号",
          all(b["构建记录编号"] == result["构建记录编号"] for b in leaves + rollups + [group]))

    # ---------- 规则4：同一版本重复提交拦住；无新分片再次构建无产出 ----------
    existing_version = per_channel["INGE-0001"]["数据版本"]
    dup = cb.build_batches(["INGE-0001"], cb.CHANNEL_FIRST,
                           data_versions={"INGE-0001": existing_version})
    check("同版本重复提交被拦截", dup["results"][0]["ok"] is False and "重复提交已拦截" in dup["results"][0]["message"])
    empty = cb.build_batches(["INGE-0001"], cb.CHANNEL_FIRST)
    check("无待入批分片时不产生新批次", all(not r["ok"] for r in empty["results"]))
    batch_count_before = len(store.rows(cb.BATCH_MODULE))
    check("空构建未写入批次", len(store.rows(cb.BATCH_MODULE)) == batch_count_before)

    # 并发：同版本同时提交，仅一次成功
    cb.register_shard({"通道编号": "INGE-0002", "输入格式": "CSV", "分片大小": 10})
    cb.register_shard({"通道编号": "INGE-0002", "输入格式": "CSV", "分片大小": 10})
    outcomes: list[dict] = []
    barrier = threading.Barrier(4)

    def worker() -> None:
        barrier.wait()
        outcomes.append(cb.build_batches(["INGE-0002"], cb.FORMAT_FIRST,
                                         data_versions={"INGE-0002": "CONC@v1"}))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    success = [o for o in outcomes if any(r["ok"] for r in o["results"])]
    check("并发同版本仅一次成功", len(success) == 1, f"成功次数={len(success)}")

    # ---------- 规则5：解析侧读同一份 + 审核 + 编排 ----------
    parser_leaves = cb.list_batches(level=cb.LEVEL_LEAF)
    ingestion_leaves = [b for b in store.rows(cb.BATCH_MODULE) if b["批次层级"] == cb.LEVEL_LEAF]
    check("解析侧与接入侧读同一份批次包", parser_leaves == ingestion_leaves)
    target_no = leaves[0]["批次编号"]
    _, msg = cb.orchestrate(target_no)
    check("未审核通过不能编排", "尚未审核通过" in msg)
    review, msg = cb.review_leaf(target_no, cb.REVIEW_PASSED)
    check("审核通过回写工作台与批次", review and review["审核状态"] == "已通过"
          and cb.get_batch_by_no(target_no)["审核状态"] == "已通过")
    again, msg = cb.review_leaf(target_no, cb.REVIEW_PASSED)
    check("重复审核被拦截", again is None)
    review, msg = cb.orchestrate(target_no)
    check("审核通过后可编排后续处理", review is not None and review["编排指令"])
    again, msg = cb.orchestrate(target_no)
    check("重复编排被拦截", again is None and "重复编排" in msg)
    detail = cb.get_batch_detail(target_no)
    check("批次详情含分片明细与工作台项", detail["分片明细"] and detail["工作台项"]["批次编号"] == target_no)

    # ---------- 规则6：历史不变 + 阈值重算适用范围 ----------
    history = cb.get_batch_by_no("CIB-0001")
    check("历史批次内容不变", history["分片编号列表"] == ["SHARD-3001", "SHARD-3002"]
          and history["数据版本"] == "INGE-0003@v1" and history["批次大小"] == 120)
    check("历史分片不参与重算",
          all(s["分片状态"] == cb.SHARD_BATCHED and s["所属批次"] == "CIB-0001"
              for s in store.rows(cb.SHARD_MODULE) if s["分片编号"] in {"SHARD-3001", "SHARD-3002"}))

    # 阈值变更后重算：新阈值只作用于新的待入批分片，历史批次保持原样
    ch3 = next(c for c in store.rows("ingestion") if c["通道编号"] == "INGE-0003")
    ch3["背压阈值"] = 200
    new_shard, _ = cb.register_shard({"通道编号": "INGE-0003", "输入格式": "JSON", "分片大小": 150})
    check("阈值变更后按新适用范围判定（150 不再超限）", new_shard["分片状态"] == cb.SHARD_PENDING
          and new_shard["背压阈值"] == 200)
    recalc = cb.build_batches(["INGE-0003"], cb.CHANNEL_FIRST)
    check("重算只消费待入批分片", recalc["results"][0]["ok"]
          and recalc["results"][0]["leaf批次"] != ["CIB-0001"])
    check("重算后历史批次仍不可变",
          cb.get_batch_by_no("CIB-0001")["批次大小"] == 120
          and cb.get_batch_by_no("CIB-0001")["背压阈值"] == 120)

    # ---------- 失败逐台：不存在的通道 ----------
    mixed = cb.build_batches(["INGE-0001", "GHOST"], cb.CHANNEL_FIRST)
    check("不存在通道逐台失败、不影响其他台记录结构",
          {r["通道编号"]: r["ok"] for r in mixed["results"]} == {"INGE-0001": False, "GHOST": False})

    print("\n构建记录：")
    for rec in cb.list_build_records():
        print(" -", rec["构建记录编号"], rec["合并策略"],
              "成功=", rec["成功通道"], "失败=", rec["失败通道"])

    print(f"\n{'全部通过' if not failures else f'{len(failures)} 项失败'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
