"""分发看板领域服务：把源事件、初投冻结目标、接收回执和补发记录聚合成目标覆盖数据集。

几个不能含糊的约束：

* 预期目标在**初投时冻结**。此后 route_dispatcher 改配只影响后续事件，
  不能改写既有事件的预期集合，更不能抹掉已经产生的收发证据。
* 接收凭证按 ``(源事件, 目标)`` 去重。初投确认、迟到确认、补发确认无论谁先到、
  是否并发，都只记一次“完整接收”，其余全部落流水但标记为重复。
* 回放范围由**领取时的目标清单**决定。领取之后、重放之前到达的回执属于迟到确认：
  它会把该目标从本轮重放范围里关掉，但不计入实际补发次数。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any

from app.pipeline import pipeline_bus
from app.seed import SEED_ROWS
from app.services.change_event import ChangeEventService
from app.store import store

# 初投时的目标安排：在路由规则 version=1 下冻结，之后不随改配变化。
INITIAL_DISPATCH: dict[str, list[str]] = {
    "evt-inv-20261119-0001": ["warehouse-east", "warehouse-west"],
    "evt-inv-20261119-0002": ["warehouse-east", "warehouse-west"],
    "evt-ord-20261119-0001": ["warehouse-east"],
    "evt-oit-20261119-0001": ["warehouse-east", "warehouse-west"],
    "evt-pay-20261119-0001": ["warehouse-west"],
}

INITIAL_RULE_BY_TABLE: dict[str, str] = {
    "inventory_adjustments": "库存调整分发",
    "orders": "订单事件分发",
    "order_items": "订单明细分发",
    "payments": "支付流水分发",
}

INITIAL_DISPATCH_AT = "2026-11-19T22:14:02+08:00"
INITIAL_RULE_VERSION = 1

# 初投阶段已经回执的目标；warehouse-west 对 inventory_adjustments 的确认尚未到达。
INITIAL_RECEIPTS: list[tuple[str, str, str, str]] = [
    # (event_id, target, channel, received_at)
    ("evt-inv-20261119-0001", "warehouse-east", "kafka://wh-east/inventory", "2026-11-19T22:14:05+08:00"),
    ("evt-inv-20261119-0002", "warehouse-east", "kafka://wh-east/inventory", "2026-11-19T22:31:20+08:00"),
    ("evt-inv-20261119-0002", "warehouse-west", "kafka://wh-west/inventory", "2026-11-19T22:31:22+08:00"),
    ("evt-ord-20261119-0001", "warehouse-east", "kafka://wh-east/orders", "2026-11-19T21:58:15+08:00"),
    ("evt-oit-20261119-0001", "warehouse-east", "kafka://wh-east/order_items", "2026-11-19T21:58:16+08:00"),
    ("evt-oit-20261119-0001", "warehouse-west", "kafka://wh-west/order_items", "2026-11-19T21:58:18+08:00"),
    ("evt-pay-20261119-0001", "warehouse-west", "kafka://wh-west/payments", "2026-11-19T22:44:02+08:00"),
]

PAIR_KEY = tuple[str, str]


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class TargetPlan:
    """一条 (源事件, 目标) 的初投预期，产生后不可被改配修改。"""

    def __init__(self, dispatch_id: str, event: dict[str, Any], target: str) -> None:
        self.dispatch_id = dispatch_id
        self.event_id = str(event["事件ID"])
        self.target = target
        self.rule_name = INITIAL_RULE_BY_TABLE.get(str(event.get("source_table", "")), "")
        self.rule_version = INITIAL_RULE_VERSION
        self.frozen_at = INITIAL_DISPATCH_AT
        self.source_db = str(event.get("source_db", ""))
        self.source_table = str(event.get("source_table", ""))
        self.operation = str(event.get("操作类型", ""))
        self.tx_id = str(event.get("事务ID", ""))
        self.binlog_file = str(event.get("binlog_file", ""))
        self.binlog_pos = int(event.get("binlog_pos", 0))
        self.gtid = str(event.get("gtid", ""))
        self.event_time = str(event.get("事件时间", ""))

    def source_coordinates(self) -> dict[str, Any]:
        return {
            "source_db": self.source_db,
            "source_table": self.source_table,
            "operation": self.operation,
            "tx_id": self.tx_id,
            "binlog_file": self.binlog_file,
            "binlog_pos": self.binlog_pos,
            "gtid": self.gtid,
            "event_time": self.event_time,
        }


class ClaimPair:
    def __init__(self, event_id: str, target: str) -> None:
        self.event_id = event_id
        self.target = target
        # 待重放 → 已补发 / 已由迟到确认关闭
        self.state = "待重放"
        self.replay_count = 0
        self.replayed_at: str | None = None
        self.closed_by_receipt: str | None = None
        self.closed_at: str | None = None


class Claim:
    def __init__(self, claim_id: str, operator: str, claimed_at: str, pairs: list[ClaimPair]) -> None:
        self.claim_id = claim_id
        self.operator = operator
        self.claimed_at = claimed_at
        self.pairs = pairs

    def find(self, event_id: str, target: str) -> ClaimPair | None:
        for pair in self.pairs:
            if pair.event_id == event_id and pair.target == target:
                return pair
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "operator": self.operator,
            "claimed_at": self.claimed_at,
            "total": len(self.pairs),
            "pending": sum(1 for p in self.pairs if p.state == "待重放"),
            "replayed": sum(1 for p in self.pairs if p.state == "已补发"),
            "closed_by_late_receipt": sum(1 for p in self.pairs if p.state == "已由迟到确认关闭"),
            "actual_replays": sum(p.replay_count for p in self.pairs),
            "pairs": [
                {
                    "event_id": p.event_id,
                    "target": p.target,
                    "state": p.state,
                    "replay_count": p.replay_count,
                    "replayed_at": p.replayed_at,
                    "closed_by_receipt": p.closed_by_receipt,
                    "closed_at": p.closed_at,
                }
                for p in self.pairs
            ],
        }


class DispatchBoard:
    """分发看板状态机：冻结计划、回执去重、缺投领取、重放登记。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.reset()

    # ------------------------------------------------------------------ 初始化
    def reset(self) -> None:
        with self._lock:
            # 演练重置同时还原路由规则，避免上次改配污染初投冻结对照。
            store.replace_rows("route_dispatcher", copy.deepcopy(SEED_ROWS["route_dispatcher"]))
            self._plans: dict[PAIR_KEY, TargetPlan] = {}
            self._receipts: dict[PAIR_KEY, dict[str, Any]] = {}
            self._receipt_log: list[dict[str, Any]] = []
            self._claims: list[Claim] = []
            self._resends: list[dict[str, Any]] = []
            self._seq = 0
            for event in self._source_events():
                event_id = str(event["事件ID"])
                for index, target in enumerate(INITIAL_DISPATCH.get(event_id, []), start=1):
                    dispatch_id = f"dsp-{event_id.split('-')[-1]}-{index:02d}"
                    self._plans[(event_id, target)] = TargetPlan(dispatch_id, event, target)
            for event_id, target, channel, received_at in INITIAL_RECEIPTS:
                self._apply_receipt(
                    event_id=event_id,
                    target=target,
                    channel=channel,
                    received_at=received_at,
                    requested_kind="初投确认",
                    note="初投阶段回执",
                )

    def _source_events(self) -> list[dict[str, Any]]:
        """按事件ID排好序的源事件行，供冻结计划和回放集合共用。"""
        rows = store.rows("change_event")
        return sorted(rows, key=lambda row: str(row.get("事件ID", "")))

    def _event(self, event_id: str) -> dict[str, Any] | None:
        for row in store.rows("change_event"):
            if str(row.get("事件ID")) == event_id:
                return row
        return None

    def _next_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq:04d}"

    # ------------------------------------------------------------------ 回执
    def record_receipt(
        self,
        *,
        event_id: str,
        target: str,
        channel: str = "",
        received_at: str | None = None,
        requested_kind: str = "回执",
        note: str = "",
    ) -> dict[str, Any]:
        """登记接收凭证；按 (源事件, 目标) 幂等，重复确认只落流水。"""
        with self._lock:
            plan = self._plans.get((event_id, target))
            if plan is None:
                return {
                    "ok": False,
                    "message": f"目标 {target} 不在源事件 {event_id} 的初投预期内，拒绝登记",
                }
            receipt = self._apply_receipt(
                event_id=event_id,
                target=target,
                channel=channel,
                received_at=received_at or _now(),
                requested_kind=requested_kind,
                note=note,
            )
            return {
                "ok": True,
                "message": (
                    f"{event_id} → {target} 已计为完整接收"
                    if receipt["applied"]
                    else f"{event_id} → {target} 已有完整接收，本次仅登记流水"
                ),
                "receipt": receipt,
                "coverage": self._coverage_row(plan),
            }

    def _apply_receipt(
        self,
        *,
        event_id: str,
        target: str,
        channel: str,
        received_at: str,
        requested_kind: str,
        note: str,
    ) -> dict[str, Any]:
        key = (event_id, target)
        existing = self._receipts.get(key)
        claim, claim_pair = self._find_claim_pair(event_id, target)
        applied = existing is None
        if not applied:
            kind = "重复确认"
            dedup_reason = "同一源事件+目标已存在完整接收，本次不计入覆盖"
        elif claim_pair is not None and claim_pair.state == "待重放":
            kind = "迟到确认"
            dedup_reason = ""
            claim_pair.state = "已由迟到确认关闭"
            claim_pair.closed_at = received_at
        elif claim_pair is not None and claim_pair.state == "已补发":
            kind = "补发确认"
            dedup_reason = ""
        elif any(r["event_id"] == event_id and r["target"] == target for r in self._resends):
            kind = "补发确认"
            dedup_reason = ""
        else:
            kind = requested_kind if requested_kind in {"初投确认", "补发确认"} else "初投确认"
            dedup_reason = ""

        receipt = {
            "receipt_id": self._next_id("rcp"),
            "event_id": event_id,
            "target": target,
            "kind": kind,
            "requested_kind": requested_kind,
            "channel": channel,
            "received_at": received_at,
            "applied": applied,
            "counted_as_complete": applied,
            "dedup_reason": dedup_reason,
            "note": note,
        }
        if applied:
            self._receipts[key] = receipt
            # 只有“领取后、重放前”到达的确认才记录为关闭凭证；
            # 已补发目标的确认只体现为一条完整接收，不改变重放计数。
            if claim_pair is not None and kind == "迟到确认":
                claim_pair.closed_by_receipt = receipt["receipt_id"]
        self._receipt_log.append(receipt)
        if applied:
            pipeline_bus.write_audit(
                "接收登记", f"{event_id}/{target}",
                f"{kind}：{event_id} → {target}",
                "dispatch_board", "已接收",
            )
        return receipt

    # ------------------------------------------------------------------ 缺投领取
    def claim_missing(self, *, operator: str, note: str = "") -> dict[str, Any]:
        """领取当前仍缺投的 (事件, 目标)。领取清单就是本轮重放范围的边界。"""
        with self._lock:
            claimed_keys = {
                (pair.event_id, pair.target)
                for claim in self._claims
                for pair in claim.pairs
            }
            pairs = [
                ClaimPair(event_id, target)
                for (event_id, target), _plan in sorted(self._plans.items())
                if (event_id, target) not in self._receipts
                and (event_id, target) not in claimed_keys
            ]
            if not pairs:
                return {"ok": False, "message": "当前没有仍缺投的事件与目标，无需领取"}
            claim = Claim(
                claim_id=f"clm-{len(self._claims) + 1:04d}",
                operator=operator or "晚班值守",
                claimed_at=_now(),
                pairs=pairs,
            )
            self._claims.append(claim)
            pipeline_bus.write_audit(
                "缺投领取", claim.claim_id,
                f"{claim.operator} 领取 {len(pairs)} 个仍缺投目标，本轮重放范围以该清单为准",
                "dispatch_board", "已领取",
            )
            result = self._claim_result(claim)
            result.update({"ok": True, "message": f"已领取 {len(pairs)} 个缺投目标"})
            return result

    def _claim_result(self, claim: Claim) -> dict[str, Any]:
        return {
            "claim": claim.as_dict(),
            "missing_after_claim": self._missing_rows(),
        }

    def _find_claim_pair(self, event_id: str, target: str) -> tuple[Claim | None, ClaimPair | None]:
        for claim in self._claims:
            pair = claim.find(event_id, target)
            if pair is not None:
                return claim, pair
        return None, None

    # ------------------------------------------------------------------ 重放登记
    def register_replays(
        self,
        *,
        claim_id: str,
        pairs: list[dict[str, str]] | None = None,
        operator: str = "",
        replayed_at: str | None = None,
    ) -> dict[str, Any]:
        """按领取清单登记实际补发；领取后到达的迟到确认会从本轮范围中剔除。"""
        with self._lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None:
                return {"ok": False, "message": f"领取单 {claim_id} 不存在"}
            requested = (
                [(p.get("event_id", ""), p.get("target", "")) for p in pairs]
                if pairs
                else [(p.event_id, p.target) for p in claim.pairs if p.state == "待重放"]
            )
            at = replayed_at or _now()
            results: list[dict[str, Any]] = []
            for event_id, target in requested:
                claim_pair = claim.find(event_id, target)
                if claim_pair is None:
                    results.append({
                        "event_id": event_id, "target": target, "applied": False,
                        "replay_count": 0, "reason": "不在本领取单的目标清单内，拒绝补发",
                    })
                    continue
                if claim_pair.state == "已由迟到确认关闭":
                    results.append({
                        "event_id": event_id, "target": target, "applied": False,
                        "replay_count": 0,
                        "reason": "领取后收到迟到确认，该目标已完整接收，本轮不重放",
                    })
                    continue
                if claim_pair.state == "已补发":
                    results.append({
                        "event_id": event_id, "target": target, "applied": False,
                        "replay_count": 0, "reason": "该目标已登记过补发，重复请求已忽略",
                    })
                    continue
                claim_pair.state = "已补发"
                claim_pair.replay_count = 1
                claim_pair.replayed_at = at
                resend = {
                    "resend_id": self._next_id("rsd"),
                    "claim_id": claim.claim_id,
                    "event_id": event_id,
                    "target": target,
                    "operator": operator or claim.operator,
                    "replayed_at": at,
                }
                self._resends.append(resend)
                results.append({
                    "event_id": event_id, "target": target, "applied": True,
                    "replay_count": 1, "reason": "已登记实际补发", "resend": resend,
                })
            actual = sum(item["replay_count"] for item in results)
            if actual:
                pipeline_bus.write_audit(
                    "补发登记", claim.claim_id,
                    f"本轮实际补发 {actual} 个目标，迟到确认关闭 "
                    f"{sum(1 for p in claim.pairs if p.state == '已由迟到确认关闭')} 个",
                    "dispatch_board", "已补发",
                )
            return {
                "ok": True,
                "message": f"本轮实际补发 {actual} 次",
                "claim": claim.as_dict(),
                "results": results,
                "actual_replays": actual,
            }

    # ------------------------------------------------------------------ 改配
    def change_route_config(
        self, *, rule_id: int, targets: list[str], note: str = "",
    ) -> dict[str, Any]:
        """模拟后续改配：只改当前路由规则，冻结的初投预期保持不动。"""
        with self._lock:
            rule = store.find("route_dispatcher", rule_id)
            if rule is None:
                return {"ok": False, "message": f"路由规则 {rule_id} 不存在"}
            normalized = [target.strip() for target in targets if target.strip()]
            if not normalized:
                return {"ok": False, "message": "目标通道不能为空"}
            rule["目标通道"] = ",".join(normalized)
            rule["是否广播"] = "是" if len(normalized) > 1 else "否"
            rule["config_version"] = int(rule.get("config_version", 1)) + 1
            rule["config_note"] = note
            pipeline_bus.write_audit(
                "路由改配", str(rule.get("规则名称")),
                f"规则改配为 {normalized}，不影响既有事件的冻结预期与收发证据",
                "dispatch_board", "已改配",
            )
            return {
                "ok": True,
                "message": f"规则 {rule.get('规则名称')} 已改配为 {', '.join(normalized)}",
                "rule": rule,
                "config_drift": self._config_drift(),
            }

    # ------------------------------------------------------------------ 聚合视图
    def _coverage_row(self, plan: TargetPlan) -> dict[str, Any]:
        key = (plan.event_id, plan.target)
        receipt = self._receipts.get(key)
        claim, claim_pair = self._find_claim_pair(plan.event_id, plan.target)
        event = self._event(plan.event_id) or {}
        return {
            "dispatch_id": plan.dispatch_id,
            "event_id": plan.event_id,
            **plan.source_coordinates(),
            "target": plan.target,
            "rule_name": plan.rule_name,
            "rule_version": plan.rule_version,
            "frozen_at": plan.frozen_at,
            "expected": True,
            "received": receipt is not None,
            "coverage_status": "已接收" if receipt else "缺投",
            "receipt_id": receipt["receipt_id"] if receipt else None,
            "receipt_kind": receipt["kind"] if receipt else None,
            "received_at": receipt["received_at"] if receipt else None,
            "receipt_channel": receipt["channel"] if receipt else None,
            "replay_count": claim_pair.replay_count if claim_pair else 0,
            "claim_id": claim.claim_id if claim else None,
            "claim_state": claim_pair.state if claim_pair else None,
            "before_image": event.get("变更前镜像", ""),
            "after_image": event.get("变更后镜像", ""),
        }

    def _missing_rows(self) -> list[dict[str, Any]]:
        """仍缺投的 (事件, 目标)：既没有接收凭证，也不在本轮已领取清单里。"""
        claimed_keys = {
            (pair.event_id, pair.target)
            for claim in self._claims
            for pair in claim.pairs
            if pair.state == "待重放"
        }
        return [
            self._coverage_row(plan)
            for key, plan in sorted(self._plans.items())
            if key not in self._receipts and key not in claimed_keys
        ]

    def _target_summary(self) -> list[dict[str, Any]]:
        targets = sorted({plan.target for plan in self._plans.values()})
        summary: list[dict[str, Any]] = []
        for target in targets:
            plans = [plan for plan in self._plans.values() if plan.target == target]
            summary.append({
                "target": target,
                "expected": len(plans),
                "complete": sum(1 for p in plans if (p.event_id, p.target) in self._receipts),
                "missing": sum(1 for p in plans if (p.event_id, p.target) not in self._receipts),
                "replays": sum(
                    pair.replay_count
                    for claim in self._claims
                    for pair in claim.pairs
                    if pair.target == target
                ),
            })
        return summary

    def _config_drift(self) -> list[dict[str, Any]]:
        drift: list[dict[str, Any]] = []
        rules = {str(rule.get("规则名称")): rule for rule in store.rows("route_dispatcher")}
        for plan in sorted(self._plans.values(), key=lambda item: (item.event_id, item.target)):
            rule = rules.get(plan.rule_name)
            if rule is None:
                continue
            current = [part.strip() for part in str(rule.get("目标通道", "")).split(",") if part.strip()]
            frozen = sorted(
                {p.target for p in self._plans.values() if p.event_id == plan.event_id}
            )
            drift.append({
                "event_id": plan.event_id,
                "rule_name": plan.rule_name,
                "rule_version": int(rule.get("config_version", 1)),
                "current_targets": current,
                "frozen_targets": frozen,
                "changed": sorted(current) != sorted(frozen),
                "note": rule.get("config_note", ""),
            })
        # 同一事件的每个目标行会重复，按事件去重后保留一份。
        unique: dict[str, dict[str, Any]] = {}
        for item in drift:
            unique.setdefault(item["event_id"], item)
        return list(unique.values())

    def source_events(self) -> list[dict[str, Any]]:
        """可回放的源事件集合：由变更事件服务提供，这里只补冻结目标与覆盖进度。"""
        with self._lock:
            events: list[dict[str, Any]] = []
            for event in ChangeEventService().replayable_event_set():
                event_id = event["event_id"]
                plans = sorted(
                    (plan for (key_event, _t), plan in self._plans.items() if key_event == event_id),
                    key=lambda plan: plan.target,
                )
                if not plans:
                    continue
                events.append({
                    **event,
                    "frozen_targets": [plan.target for plan in plans],
                    "expected_pairs": len(plans),
                    "complete_pairs": sum(
                        1 for plan in plans if (plan.event_id, plan.target) in self._receipts
                    ),
                })
            return events

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            coverage = [
                self._coverage_row(plan) for _key, plan in sorted(self._plans.items())
            ]
            complete = len(self._receipts)
            expected = len(self._plans)
            unclaimed = len(self._missing_rows())
            late = sum(1 for r in self._receipt_log if r["kind"] == "迟到确认")
            duplicates = sum(1 for r in self._receipt_log if not r["applied"])
            actual_replays = sum(pair.replay_count for claim in self._claims for pair in claim.pairs)
            return {
                "summary": {
                    "source_events": len({plan.event_id for plan in self._plans.values()}),
                    "expected_pairs": expected,
                    "complete_pairs": complete,
                    # missing_pairs 指仍无接收凭证的缺口；unclaimed_pairs 指尚未被领取的缺口。
                    "missing_pairs": expected - complete,
                    "unclaimed_pairs": unclaimed,
                    "late_receipts": late,
                    "duplicate_receipts": duplicates,
                    "claims": len(self._claims),
                    "actual_replays": actual_replays,
                },
                "targets": self._target_summary(),
                "coverage": coverage,
                "missing": self._missing_rows(),
                "claims": [claim.as_dict() for claim in self._claims],
                "receipts": list(reversed(self._receipt_log)),
                "resends": list(reversed(self._resends)),
                "source_events": self.source_events(),
                "config_drift": self._config_drift(),
            }


board = DispatchBoard()
