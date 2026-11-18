"""格式解析器接口：维护解析配置，覆盖提交配置、启用解析、废弃解析等动作。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult, ReviewPayload
from app.services.parser import ParserService

router = APIRouter(prefix="/api/parser", tags=["格式解析器"])

service = ParserService()

LIST_FIELDS = ["解析编号", "输入格式", "输出格式", "字段映射", "类型转换", "空值处理", "异常策略", "解析状态"]
STATUSES = ["草稿", "已配置", "生效中", "已废弃"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按解析编号检索"),
    status: str | None = Query(default=None, description="草稿、已配置、生效中、已废弃"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、解析编号关键字与状态过滤格式解析器列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="parser.csv"'})


# ==================== 通道接入批次包（格式解析服务边界：读取同一份 + 工作台回写） ====================

@router.get("/channel-batches/list", response_model=dict)
def list_channel_batches(
    input_format: str | None = Query(default=None, description="按输入格式过滤，解析端据此选择字段映射"),
) -> dict[str, Any]:
    """读取接入通道服务写入的同一份通道接入批次包，只暴露背压完整的 leaf 批次。"""
    items = service.list_channel_batches(input_format=input_format)
    return {"items": items, "total": len(items)}


@router.get("/channel-batches/{batch_no}/detail", response_model=dict)
def get_channel_batch(batch_no: str) -> dict[str, Any]:
    """批次包详情：含通道/背压快照、分片来源与建议解析配置。"""
    detail = service.get_channel_batch(batch_no)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"通道接入批次包 {batch_no} 不存在")
    return detail


@router.get("/review-workbench/list", response_model=dict)
def list_review_items(status: str | None = Query(default=None, description="待审核/已通过/已驳回")) -> dict[str, Any]:
    """解析侧待审核工作台：与接入通道侧批次包同步生成。"""
    items = service.list_review_items(status=status)
    return {"items": items, "total": len(items)}


@router.post("/channel-batches/{batch_no}/review", response_model=ActionResult)
def review_channel_batch(batch_no: str, payload: ReviewPayload) -> ActionResult:
    """对 leaf 批次执行审核（通过/驳回），回写工作台项与批次审核状态。"""
    entry, message = service.review_batch(batch_no, payload.action.strip(), payload.remark)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/channel-batches/{batch_no}/orchestrate", response_model=ActionResult)
def orchestrate_channel_batch(batch_no: str, payload: ReviewPayload) -> ActionResult:
    """按通道接入批次包编排接入通道后续处理；未审核通过或重复编排都会被拦住。"""
    entry, message = service.orchestrate_batch(batch_no, payload.instruction)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条解析配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"解析配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条解析配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="解析配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条解析配置执行提交配置、启用解析、废弃解析；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
