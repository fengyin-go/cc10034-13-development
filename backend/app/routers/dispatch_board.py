"""分发看板接口：源事件回放集合、目标覆盖数据集、缺投领取与补发登记。

固定路径 /api/dispatch_board/... 必须在通用资源路由 /api/{module} 之前注册，
否则会被动态路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.dispatch_board import board

router = APIRouter(prefix="/api/dispatch_board", tags=["分发看板"])


class ReceiptPayload(BaseModel):
    event_id: str
    target: str
    channel: str = ""
    received_at: str | None = None
    note: str = ""


class ClaimPayload(BaseModel):
    operator: str = "晚班值守"
    note: str = ""


class ReplayPair(BaseModel):
    event_id: str
    target: str


class ReplayPayload(BaseModel):
    pairs: list[ReplayPair] = Field(default_factory=list)
    operator: str = ""
    replayed_at: str | None = None


class RouteConfigPayload(BaseModel):
    rule_id: int
    targets: list[str]
    note: str = ""


@router.get("")
def get_board() -> dict[str, Any]:
    """整块看板：汇总指标、目标覆盖、缺投明细、领取单、回执流水、源事件集合。"""
    return board.snapshot()


@router.get("/target-coverage")
def get_target_coverage() -> dict[str, Any]:
    """路由分发视角的目标覆盖数据集：逐目标核对初投预期、接收与补发。"""
    from app.services.route_dispatcher import RouteDispatcherService

    return RouteDispatcherService().coverage_dataset()


@router.get("/source-events")
def list_source_events() -> dict[str, Any]:
    """可回放的源事件集合：Binlog 坐标、前后镜像和冻结目标。"""
    events = board.source_events()
    return {"items": events, "total": len(events)}


@router.get("/missing")
def list_missing() -> dict[str, Any]:
    """当前仍缺投的 (源事件, 目标) 明细。"""
    missing = board.snapshot()["missing"]
    return {"items": missing, "total": len(missing)}


@router.post("/receipts")
def record_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    """登记接收凭证，按 (源事件, 目标) 幂等去重。"""
    result = board.record_receipt(
        event_id=payload.event_id,
        target=payload.target,
        channel=payload.channel,
        received_at=payload.received_at,
        note=payload.note,
    )
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/claims")
def claim_missing(payload: ClaimPayload) -> dict[str, Any]:
    """领取仍缺投的 (事件, 目标)，领取清单即本轮重放范围。"""
    result = board.claim_missing(operator=payload.operator, note=payload.note)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.post("/claims/{claim_id}/replays")
def register_replays(claim_id: str, payload: ReplayPayload) -> dict[str, Any]:
    """按领取单登记实际补发；迟到确认会从本轮范围中剔除。"""
    result = board.register_replays(
        claim_id=claim_id,
        pairs=[pair.model_dump() for pair in payload.pairs] if payload.pairs else None,
        operator=payload.operator,
        replayed_at=payload.replayed_at,
    )
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/route-config")
def change_route_config(payload: RouteConfigPayload) -> dict[str, Any]:
    """后续改配入口：证明改配不会改写初投冻结目标和收发证据。"""
    result = board.change_route_config(
        rule_id=payload.rule_id, targets=payload.targets, note=payload.note)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/reset")
def reset_board() -> dict[str, Any]:
    """把看板恢复到初投后的演示状态，便于重复演练。"""
    board.reset()
    return {"ok": True, "message": "分发看板已重置为初投状态", "board": board.snapshot()}
