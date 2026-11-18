"""数据接入通道 时间线服务：生成作业时间序列、示例数据与通道接入批次包取数。"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.services import channel_batch as cb
from app.store import store

MODULE = "ingestion"
REQUIRED_FIELDS = ["通道编号", "通道名称", "输入格式"]
STATUS_ORDER = ["待开通", "开通中", "运行中", "已关闭"]
ACTION_RULES = {"开通通道": "开通中", "暂停通道": "运行中", "关闭通道": "已关闭"}
NEGATIVE_ACTIONS = []


class IngestionService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"接入通道 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据接入通道可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"接入通道已{action}"

    # ---- 通道接入批次包：接入通道服务侧（写入方） ----
    def list_shards(self, channel_code: str | None = None) -> list[dict[str, Any]]:
        return cb.list_shards(channel_code=channel_code)

    def register_shard(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        return cb.register_shard(values)

    def preview_batches(self, channel_codes: list[str], strategy: str) -> tuple[dict[str, Any] | None, str]:
        if not channel_codes:
            return None, "请至少选择一个接入通道"
        unknown = [c for c in channel_codes if cb._find_channel(c) is None]
        if unknown:
            return None, f"通道不存在：{'、'.join(unknown)}"
        if strategy not in cb.STRATEGIES:
            return None, "合并策略只支持 channel_first / format_first"
        return cb.preview_batches(channel_codes, strategy), ""

    def build_batches(self, channel_codes: list[str], strategy: str,
                      data_versions: dict[str, str] | None = None) -> tuple[dict[str, Any] | None, str]:
        if not channel_codes:
            return None, "请至少选择一个接入通道"
        unknown = [c for c in channel_codes if cb._find_channel(c) is None]
        if unknown:
            return None, f"通道不存在：{'、'.join(unknown)}"
        if strategy not in cb.STRATEGIES:
            return None, "合并策略只支持 channel_first / format_first"
        result = cb.build_batches(channel_codes, strategy, data_versions=data_versions)
        if all(not item["ok"] for item in result["results"]):
            return result, "所选通道均未生成批次，详见逐台记录"
        return result, ""

    def list_batches(self, level: str | None = None, channel_code: str | None = None) -> list[dict[str, Any]]:
        return cb.list_batches(level=level, channel_code=channel_code)

    def get_batch_detail(self, batch_no: str) -> dict[str, Any] | None:
        return cb.get_batch_detail(batch_no)

    def list_build_records(self) -> list[dict[str, Any]]:
        return cb.list_build_records()
