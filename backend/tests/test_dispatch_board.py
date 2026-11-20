"""分发看板领域逻辑自测（标准库 unittest，不依赖 pytest）。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
"""
from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.dispatch_board import board  # noqa: E402

EVENT = "evt-inv-20261119-0001"
WEST = "warehouse-west"


class DispatchBoardTest(unittest.TestCase):
    def setUp(self) -> None:
        board.reset()

    # ---------------------------------------------------------------- 初投冻结
    def test_initial_state_has_single_missing_pair(self) -> None:
        snap = board.snapshot()
        self.assertEqual(snap["summary"]["expected_pairs"], 8)
        self.assertEqual(snap["summary"]["missing_pairs"], 1)
        missing = snap["missing"]
        self.assertEqual(len(missing), 1)
        self.assertEqual((missing[0]["event_id"], missing[0]["target"]), (EVENT, WEST))

    def test_route_config_change_keeps_frozen_targets_and_evidence(self) -> None:
        before = board.snapshot()
        result = board.change_route_config(
            rule_id=1, targets=["warehouse-east"], note="后续改配")
        self.assertTrue(result["ok"])

        after = board.snapshot()
        east_rows = [
            row for row in after["coverage"]
            if row["event_id"] == EVENT and row["target"] == "warehouse-east"
        ]
        west_rows = [
            row for row in after["coverage"]
            if row["event_id"] == EVENT and row["target"] == WEST
        ]
        self.assertEqual(len(east_rows), 1)
        self.assertEqual(len(west_rows), 1)
        # 东侧已收到的凭证不因改配而消失，西侧仍按初投冻结预期算缺投。
        self.assertTrue(east_rows[0]["received"])
        self.assertFalse(west_rows[0]["received"])
        self.assertEqual(before["summary"]["complete_pairs"], after["summary"]["complete_pairs"])

        drift = {row["event_id"]: row for row in after["config_drift"]}
        self.assertEqual(drift[EVENT]["frozen_targets"], ["warehouse-east", "warehouse-west"])
        self.assertEqual(drift[EVENT]["current_targets"], ["warehouse-east"])
        self.assertTrue(drift[EVENT]["changed"])

    def test_receipt_for_target_outside_frozen_plan_is_rejected(self) -> None:
        result = board.record_receipt(
            event_id=EVENT, target="warehouse-north", channel="kafka://north/inventory")
        self.assertFalse(result["ok"])
        self.assertIn("初投预期", result["message"])

    # ---------------------------------------------------------------- 回执幂等
    def test_receipt_is_idempotent_per_event_and_target(self) -> None:
        first = board.record_receipt(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        second = board.record_receipt(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        self.assertTrue(first["receipt"]["applied"])
        self.assertFalse(second["receipt"]["applied"])
        self.assertEqual(second["receipt"]["kind"], "重复确认")
        snap = board.snapshot()
        self.assertEqual(snap["summary"]["complete_pairs"], 8)
        self.assertEqual(snap["summary"]["duplicate_receipts"], 1)

    def test_source_and_target_scoped_dedupe_keeps_other_targets(self) -> None:
        # 同一源事件的另一个目标独立计数。
        board.record_receipt(event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        snap = board.snapshot()
        pairs = {(row["event_id"], row["target"]) for row in snap["coverage"] if row["received"]}
        self.assertIn((EVENT, "warehouse-east"), pairs)
        self.assertIn((EVENT, WEST), pairs)
        self.assertEqual(len(pairs), 8)

    # ---------------------------------------------------------------- 迟到确认
    def test_late_receipt_after_claim_closes_pair_and_skips_replay(self) -> None:
        claim = board.claim_missing(operator="晚班值守")
        self.assertTrue(claim["ok"])
        claim_id = claim["claim"]["claim_id"]

        late = board.record_receipt(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory",
            received_at="2026-11-19T23:41:07+08:00")
        self.assertEqual(late["receipt"]["kind"], "迟到确认")
        self.assertTrue(late["receipt"]["counted_as_complete"])

        replay = board.register_replays(claim_id=claim_id)
        self.assertEqual(replay["actual_replays"], 0)
        self.assertEqual(replay["claim"]["closed_by_late_receipt"], 1)
        self.assertEqual(replay["claim"]["pairs"][0]["state"], "已由迟到确认关闭")

        snap = board.snapshot()
        self.assertEqual(snap["summary"]["complete_pairs"], 8)
        self.assertEqual(snap["summary"]["missing_pairs"], 0)
        self.assertEqual(snap["summary"]["actual_replays"], 0)

    def test_replay_before_late_receipt_counts_once(self) -> None:
        claim_id = board.claim_missing(operator="晚班值守")["claim"]["claim_id"]
        replay = board.register_replays(claim_id=claim_id)
        self.assertEqual(replay["actual_replays"], 1)

        # 补发登记本身不是接收凭证；补发后到达的首次确认是该目标的第一个完整接收。
        late = board.record_receipt(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        self.assertEqual(late["receipt"]["kind"], "补发确认")
        self.assertTrue(late["receipt"]["counted_as_complete"])

        # 再来一次并发/重复确认仍然只计一次完整接收。
        duplicate = board.record_receipt(
            event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")
        self.assertEqual(duplicate["receipt"]["kind"], "重复确认")
        self.assertFalse(duplicate["receipt"]["counted_as_complete"])

        snap = board.snapshot()
        self.assertEqual(snap["summary"]["complete_pairs"], 8)
        self.assertEqual(snap["summary"]["actual_replays"], 1)
        self.assertEqual(snap["summary"]["duplicate_receipts"], 1)

    def test_duplicate_replay_request_does_not_inflate_count(self) -> None:
        claim_id = board.claim_missing(operator="晚班值守")["claim"]["claim_id"]
        board.register_replays(claim_id=claim_id)
        again = board.register_replays(claim_id=claim_id)
        self.assertEqual(again["actual_replays"], 0)
        self.assertEqual(again["results"], [])
        self.assertEqual(board.snapshot()["summary"]["actual_replays"], 1)

    def test_replay_outside_claim_scope_is_refused(self) -> None:
        claim_id = board.claim_missing(operator="晚班值守")["claim"]["claim_id"]
        # 领取单只包含一条缺投，不能顺手重放已完成的东侧目标。
        result = board.register_replays(
            claim_id=claim_id,
            pairs=[{"event_id": EVENT, "target": "warehouse-east"}])
        self.assertEqual(result["actual_replays"], 0)
        self.assertIn("不在本领取单的目标清单内", result["results"][0]["reason"])

    # ---------------------------------------------------------------- 并发
    def test_concurrent_receipt_and_replay_count_once(self) -> None:
        claim_id = board.claim_missing(operator="晚班值守")["claim"]["claim_id"]
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        lock = threading.Lock()

        def do_receipt() -> None:
            barrier.wait()
            receipt = board.record_receipt(
                event_id=EVENT, target=WEST, channel="kafka://wh-west/inventory")["receipt"]
            with lock:
                outcomes.append(f"receipt:{receipt['applied']}")

        def do_replay() -> None:
            barrier.wait()
            replay = board.register_replays(claim_id=claim_id)
            with lock:
                outcomes.append(f"replay:{replay['actual_replays']}")

        threads = [threading.Thread(target=do_receipt), threading.Thread(target=do_replay)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        snap = board.snapshot()
        # 两种顺序都只允许一次完整接收；要么补发 1 次，要么被迟到确认关闭后 0 次。
        self.assertEqual(snap["summary"]["complete_pairs"], 8)
        self.assertIn(snap["summary"]["actual_replays"], (0, 1))
        receipt_outcome = [item for item in outcomes if item.startswith("receipt:")]
        self.assertEqual(receipt_outcome, ["receipt:True"])

    # ---------------------------------------------------------------- 源事件
    def test_source_events_are_replayable_with_coordinates(self) -> None:
        events = board.source_events()
        inventory = next(item for item in events if item["event_id"] == EVENT)
        self.assertTrue(inventory["replayable"])
        self.assertEqual(inventory["source_table"], "inventory_adjustments")
        self.assertEqual(inventory["binlog_file"], "mysql-bin.000042")
        self.assertEqual(inventory["binlog_pos"], 187654)
        self.assertTrue(inventory["gtid"].startswith("3e11c0de"))
        self.assertEqual(inventory["frozen_targets"], ["warehouse-east", WEST])
        self.assertEqual(inventory["expected_pairs"], 2)
        self.assertEqual(inventory["complete_pairs"], 1)


if __name__ == "__main__":
    unittest.main()
