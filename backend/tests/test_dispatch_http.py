"""本轮分发看板改动的回归清单（标准库 unittest，直接调用路由函数校验状态码）。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import HTTPException  # noqa: E402

from app.main import app  # noqa: E402
from app.routers import dispatch_board as routes  # noqa: E402

EVENT = "evt-inv-20261119-0001"
WEST = "warehouse-west"


class DispatchHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        routes.reset_board()

    def test_fixed_route_is_registered_before_generic_module_route(self) -> None:
        # FastAPI 把 include_router 收成嵌套路由器，这里按注册顺序摊平路径。
        paths: list[str] = []
        for route in app.routes:
            inner = getattr(route, "original_router", None)
            if inner is not None:
                paths.extend(getattr(item, "path", "") for item in inner.routes)
            else:
                paths.append(getattr(route, "path", ""))
        self.assertLess(
            paths.index("/api/dispatch_board"), paths.index("/api/{module}"))

    def test_board_endpoint_shape(self) -> None:
        payload = routes.get_board()
        for key in ("summary", "targets", "coverage", "missing", "claims",
                    "receipts", "source_events", "config_drift"):
            self.assertIn(key, payload)
        self.assertEqual(payload["summary"]["expected_pairs"], 8)
        self.assertEqual(payload["summary"]["missing_pairs"], 1)

    def test_target_coverage_endpoint_uses_frozen_plan(self) -> None:
        payload = routes.get_target_coverage()
        rows = [(r["event_id"], r["target"]) for r in payload["coverage"] if r["event_id"] == EVENT]
        self.assertEqual(rows, [(EVENT, "warehouse-east"), (EVENT, WEST)])

    def test_late_receipt_scenario(self) -> None:
        claim = routes.claim_missing(routes.ClaimPayload(operator="晚班值守"))["claim"]
        self.assertEqual(claim["total"], 1)
        self.assertEqual(claim["pairs"][0]["target"], WEST)

        receipt = routes.record_receipt(routes.ReceiptPayload(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory",
            received_at="2026-11-19T23:41:07+08:00"))["receipt"]
        self.assertEqual(receipt["kind"], "迟到确认")
        self.assertTrue(receipt["counted_as_complete"])

        replay = routes.register_replays(
            claim["claim_id"], routes.ReplayPayload())
        self.assertEqual(replay["actual_replays"], 0)
        self.assertEqual(replay["claim"]["closed_by_late_receipt"], 1)

        summary = routes.get_board()["summary"]
        self.assertEqual(summary["complete_pairs"], 8)
        self.assertEqual(summary["actual_replays"], 0)

    def test_duplicate_receipt_is_not_counted_twice(self) -> None:
        payload = routes.ReceiptPayload(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        first = routes.record_receipt(payload)["receipt"]
        second = routes.record_receipt(payload)["receipt"]
        self.assertTrue(first["counted_as_complete"])
        self.assertFalse(second["counted_as_complete"])
        summary = routes.get_board()["summary"]
        self.assertEqual(summary["complete_pairs"], 8)
        self.assertEqual(summary["duplicate_receipts"], 1)

    def test_receipt_outside_frozen_plan_returns_400(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            routes.record_receipt(routes.ReceiptPayload(
                event_id=EVENT, target="warehouse-north"))
        self.assertEqual(caught.exception.status_code, 400)

    def test_claim_without_missing_returns_409(self) -> None:
        routes.claim_missing(routes.ClaimPayload(operator="晚班值守"))
        routes.record_receipt(routes.ReceiptPayload(event_id=EVENT, target=WEST))
        with self.assertRaises(HTTPException) as caught:
            routes.claim_missing(routes.ClaimPayload(operator="晚班值守"))
        self.assertEqual(caught.exception.status_code, 409)

    def test_route_config_change_does_not_touch_frozen_evidence(self) -> None:
        before = routes.get_board()
        response = routes.change_route_config(routes.RouteConfigPayload(
            rule_id=1, targets=["warehouse-east"], note="仅东侧"))
        self.assertTrue(response["ok"])
        after = routes.get_board()
        west = next(
            r for r in after["coverage"] if r["event_id"] == EVENT and r["target"] == WEST)
        self.assertFalse(west["received"])
        self.assertEqual(before["summary"]["complete_pairs"], after["summary"]["complete_pairs"])

    def test_reset_restores_route_config_and_board(self) -> None:
        routes.change_route_config(routes.RouteConfigPayload(
            rule_id=1, targets=["warehouse-east"], note="仅东侧"))
        routes.claim_missing(routes.ClaimPayload(operator="晚班值守"))
        routes.reset_board()
        board = routes.get_board()
        self.assertEqual(board["summary"]["claims"], 0)
        self.assertEqual(board["summary"]["missing_pairs"], 1)
        drift = {row["event_id"]: row for row in board["config_drift"]}
        self.assertEqual(drift[EVENT]["current_targets"], ["warehouse-east"])
        self.assertEqual(drift[EVENT]["frozen_targets"], ["warehouse-east", WEST])


if __name__ == "__main__":
    unittest.main()
