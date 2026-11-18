"""格式解析器业务规则：状态流转、字段校验与筛选口径都收在这里。

边界：解析服务不生产批次包，只读接入通道服务写入的同一份 ingestion_batch，
并回写待审核工作台项（review_workbench）与后续处理编排指令。
"""
from __future__ import annotations

from typing import Any

from app.services import channel_batch as cb
from app.store import store

MODULE = "parser"
LIST_FIELDS = ["解析编号", "输入格式", "输出格式", "字段映射", "类型转换", "空值处理", "异常策略", "解析状态"]
REQUIRED_FIELDS = ["解析编号", "输入格式", "输出格式"]
STATUS_ORDER = ["草稿", "已配置", "生效中", "已废弃"]
ACTION_RULES = {"提交配置": "已配置", "启用解析": "生效中", "废弃解析": "已废弃"}
NEGATIVE_ACTIONS = []


class ParserService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("解析编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"解析配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于格式解析器可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"解析配置已{action}"

    # ---- 通道接入批次包：格式解析服务侧（读取方 + 工作台回写） ----
    def list_channel_batches(self, input_format: str | None = None) -> list[dict[str, Any]]:
        """读取接入通道服务写入的同一份批次包；解析端只看背压完整的 leaf 批次。"""
        return cb.list_batches(level=cb.LEVEL_LEAF, input_format=input_format)

    def get_channel_batch(self, batch_no: str) -> dict[str, Any] | None:
        return cb.get_batch_detail(batch_no)

    def list_review_items(self, status: str | None = None) -> list[dict[str, Any]]:
        return cb.list_review_items(status=status)

    def review_batch(self, batch_no: str, action: str, remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
        return cb.review_leaf(batch_no, action, remark)

    def orchestrate_batch(self, batch_no: str, instruction: str | None = None) -> tuple[dict[str, Any] | None, str]:
        return cb.orchestrate(batch_no, instruction)
