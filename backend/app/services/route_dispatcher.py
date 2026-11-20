"""路由分发领域服务：按规则将变更事件路由到下游消费通道。

上游：dedup_window(去重) → event_filter(过滤) → route_dispatcher(分发)
消费：下游Kafka/MQ消费端
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class RouteDispatcherService(BaseModuleService):
    MODULE = "route_dispatcher"
    ENTITY = "路由规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "匹配条件", "目标通道", "分发策略", "是否广播", "权重", "备用目标"]
    REQUIRED_FIELDS = ["规则名称", "匹配条件", "目标通道"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已启用"}
    NEGATIVE_ACTIONS = []

    def route_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个事件执行路由分发。

        根据规则匹配条件和分发策略，将事件定向到目标通道。
        支持广播、加权分发和备通道降级。
        """
        condition = config.get("匹配条件", "")
        target = config.get("目标通道", "default_sink")
        strategy = config.get("分发策略", "round_robin")
        broadcast = config.get("是否广播", "否") == "是"
        weight = int(config.get("权重", "1"))
        backup = config.get("备用目标", "")

        event_str = str(event)
        if condition and condition not in event_str and condition not in event.get("来源表", ""):
            return {"routed": False, "reason": "condition_mismatch"}

        route_result: dict[str, Any] = {
            "routed": True,
            "event_id": event.get("事件ID", ""),
            "rule": config.get("规则名称"),
            "strategy": strategy,
            "broadcast": broadcast,
            "routed_at": datetime.now().isoformat(),
        }

        if broadcast:
            route_result["targets"] = [target]
        else:
            route_result["targets"] = [target]
            route_result["weight"] = weight

        if backup:
            route_result["backup_target"] = backup

        pipeline_bus.routing_table.append({
            "name": config.get("规则名称"),
            "condition": condition,
            "target": target,
            "enabled": config.get("status") == "已启用",
        })

        pipeline_bus.pipeline_metrics["routed"] += 1
        pipeline_bus.write_audit(
            "路由分发", target,
            f"事件 {event.get('事件ID','')} 路由到 {target}",
            "route_dispatcher", "已路由"
        )
        return route_result

    def batch_route(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量路由事件。

        按策略对事件流执行批量分发，支持负载均衡模式。
        """
        strategy = config.get("分发策略", "round_robin")
        targets = config.get("目标通道", "default").split(",")
        weight = int(config.get("权重", "1"))
        broadcast = config.get("是否广播", "否") == "是"

        by_target: dict[str, int] = {}
        for i, event in enumerate(events):
            if broadcast:
                target = targets[0]
            else:
                idx = (i // weight) % len(targets)
                target = targets[idx]
            by_target[target] = by_target.get(target, 0) + 1

        pipeline_bus.write_audit(
            "批量路由", f"{len(events)}条事件",
            f"批量路由 {len(events)} 条事件到 {len(by_target)} 个目标",
            "route_dispatcher", "已路由"
        )
        return {
            "total_routed": len(events),
            "by_target": by_target,
            "strategy": strategy,
            "broadcast": broadcast,
        }

    def compute_route_distribution(self, configs: list[dict[str, Any]]) -> dict[str, Any]:
        """计算路由分发比例的预期分布。"""
        total_weight = sum(int(c.get("权重", "1")) for c in configs if c.get("status") == "已启用")
        dist: dict[str, Any] = {}
        for c in configs:
            if c.get("status") == "已启用":
                w = int(c.get("权重", "1"))
                name = c.get("规则名称", "")
                dist[name] = {
                    "target": c.get("目标通道"),
                    "weight": w,
                    "share_pct": round(w / max(total_weight, 1) * 100, 1),
                }
        return {"total_weight": total_weight, "distribution": dist}

    def find_fallback_route(self, event: dict[str, Any], primary_target: str) -> dict[str, Any]:
        """查找备选路由。"""
        fallback = None
        for rule in pipeline_bus.routing_table:
            if rule.get("target") == primary_target:
                pass
            elif rule.get("enabled"):
                fallback = rule
                break
        if fallback:
            return {"found": True, "fallback": fallback["target"], "condition": fallback["condition"]}
        return {"found": False}

    def coverage_dataset(self) -> dict[str, Any]:
        """按冻结的初投目标聚合目标覆盖数据集。

        路由规则只决定“后续事件该投给谁”；某条事件投给了谁、谁回了凭证，
        一律以分发看板里冻结的 (源事件, 目标) 计划为准，改配不回写历史。
        """
        from app.services.dispatch_board import board

        snapshot = board.snapshot()
        return {
            "targets": snapshot["targets"],
            "coverage": snapshot["coverage"],
            "missing": snapshot["missing"],
            "source_events": snapshot["source_events"],
            "summary": snapshot["summary"],
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            target = entry.get("目标通道", "")
            pipeline_bus.routing_table.append({
                "name": entry.get("规则名称"),
                "condition": entry.get("匹配条件"),
                "target": target,
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "路由启用", target,
                f"路由规则 {entry.get('规则名称')} 已启用，指向 {target}",
                "route_dispatcher", "已启用"
            )
        return entry, msg