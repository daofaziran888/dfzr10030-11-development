"""数据接入通道 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    BatchBuildPayload,
    BatchPreviewPayload,
    EntryPayload,
    ShardPayload,
)
from app.services.ingestion import IngestionService

router = APIRouter(prefix="/api/ingestion", tags=["数据接入通道"])
service = IngestionService()
LIST_FIELDS = ["通道编号", "通道名称", "输入格式", "解析引擎", "并发数量", "背压阈值", "缓冲队列", "通道状态"]
STATUSES = ["待开通", "开通中", "运行中", "已关闭"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=ingestion.csv"})


# ==================== 通道接入批次包（接入通道服务边界：写入方） ====================

@router.get("/shards/list")
def list_shards(channel: str | None = Query(default=None, description="按通道编号过滤")) -> dict[str, Any]:
    """查看各通道缓冲队列分片：分片绑定通道与输入格式，可核对每条数据链的来源。"""
    items = service.list_shards(channel_code=channel)
    return {"items": items, "total": len(items)}


@router.post("/shards")
def register_shard(payload: ShardPayload) -> ActionResult:
    """登记输入分片；超过通道背压阈值的分片直接置为超限拦截，不允许进入完成批次。"""
    entry_or_none, problems = service.register_shard(payload.values)
    if problems:
        return ActionResult(ok=False, message="；".join(problems))
    return ActionResult(ok=True, message="输入分片已绑定通道与格式并入缓冲队列", entry=entry_or_none)


@router.post("/batches/preview")
def preview_batches(payload: BatchPreviewPayload) -> ActionResult:
    """按输入格式和背压阈值预演切批，不落库，用于比较两种合并边界的差异。"""
    projection, message = service.preview_batches(payload.通道编号列表, payload.合并策略)
    if projection is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message="切批预演完成（未写入批次包）", entry=projection)


@router.post("/batches/build")
def build_batches(payload: BatchBuildPayload) -> ActionResult:
    """多选通道整组写入通道接入批次包；成功/失败逐台记录，同一数据版本仅一次成功。"""
    result, message = service.build_batches(payload.通道编号列表, payload.合并策略, payload.数据版本)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=not message, message=message or "通道接入批次包已生成并同步待审核工作台项", entry=result)


@router.get("/batches/list")
def list_batches(
    level: str | None = Query(default=None, description="leaf / channel / group"),
    channel: str | None = Query(default=None, description="按通道编号过滤"),
) -> dict[str, Any]:
    """读取通道接入批次包：解析服务与本服务读同一份数据。"""
    items = service.list_batches(level=level, channel_code=channel)
    return {"items": items, "total": len(items)}


@router.get("/batches/build-records")
def list_build_records() -> dict[str, Any]:
    """每次整组构建一条记录，逐台留痕，方便核对同一版本的提交结果。"""
    items = service.list_build_records()
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_no}/detail")
def get_batch_detail(batch_no: str) -> dict[str, Any]:
    """批次包详情：分片来源、背压快照、切断点与解析侧工作台项。"""
    detail = service.get_batch_detail(batch_no)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"通道接入批次包 {batch_no} 不存在")
    return detail


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"接入通道 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="接入通道已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
