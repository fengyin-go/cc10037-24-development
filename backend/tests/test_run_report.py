"""综合运行报告回归测试（标准库 unittest，不依赖额外包）。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import tempfile
import unittest

from app.engine.run_report import (
    ReceiptConflictError,
    ReceiptValidationError,
    compute_run_report,
    normalize_partition_progress,
    normalize_quality_gate,
)
from app.services.run_report import (
    ReportNotConfirmedError,
    RunNotFoundError,
    RunReportService,
)

PIPELINE = "ODS到DWD订单宽表ETL"


class RunReportEngineTests(unittest.TestCase):
    def test_row_counts_must_balance(self) -> None:
        with self.assertRaises(ReceiptValidationError):
            normalize_quality_gate({
                "total_rows": 100, "passed_rows": 90, "failed_rows": 5,
            })

    def test_partition_date_cannot_cross_days(self) -> None:
        with self.assertRaises(ReceiptConflictError):
            normalize_partition_progress(
                {"partitions": [
                    {"partition": "2026-11-16/p0", "total_rows": 10, "failed_rows": 0},
                ]},
                batch_date="2026-11-17",
            )

    def test_mismatched_gate_and_partition_totals_are_inconsistent(self) -> None:
        quality = normalize_quality_gate({"total_rows": 100, "failed_rows": 1})
        partitions = normalize_partition_progress(
            {"partitions": [
                {"partition": "2026-11-19/p0", "total_rows": 120, "failed_rows": 1},
            ]},
            batch_date="2026-11-19",
        )
        report = compute_run_report(
            run={"run_id": "r1", "pipeline_id": PIPELINE, "batch_date": "2026-11-19"},
            quality=quality,
            partitions=partitions,
        )
        self.assertEqual(report["state"], "inconsistent")
        self.assertFalse(report["evidence"]["same_round"])

    def test_missing_receipt_keeps_progress_unavailable(self) -> None:
        quality = normalize_quality_gate({"total_rows": 100, "failed_rows": 1})
        report = compute_run_report(
            run={"run_id": "r1", "pipeline_id": PIPELINE, "batch_date": "2026-11-19"},
            quality=quality,
            partitions=None,
        )
        self.assertEqual(report["state"], "partial")
        self.assertFalse(report["progress"]["available"])
        self.assertEqual(report["evidence"]["missing_receipts"], ["partition_progress"])


class RunReportServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = RunReportService(
            os.path.join(tempfile.mkdtemp(), "run_reports.sqlite3")
        )

    def _register(self, run_id: str = "run-1", batch_date: str = "2026-11-18") -> None:
        self.service.register_run(PIPELINE, {
            "run_id": run_id,
            "batch_date": batch_date,
            "expected_partitions": 2,
        })

    def _gate(self) -> dict:
        return {
            "receipt_type": "quality_gate",
            "payload": {
                "total_rows": 1000,
                "passed_rows": 990,
                "failed_rows": 10,
                "threshold": 0.01,
                "level": "WARN",
            },
        }

    def _partitions(self, expected: int = 2) -> dict:
        return {
            "receipt_type": "partition_progress",
            "payload": {
                "expected_partitions": expected,
                "partitions": [
                    {"partition": "2026-11-18/p0", "total_rows": 600, "failed_rows": 6},
                    {"partition": "2026-11-18/p1", "total_rows": 400, "failed_rows": 4},
                ],
            },
        }

    def test_register_run_is_idempotent(self) -> None:
        self._register()
        second = self.service.register_run(PIPELINE, {
            "run_id": "run-1", "batch_date": "2026-11-18",
        })
        self.assertFalse(second["created"])
        self.assertEqual(len(self.service.list_runs(PIPELINE)), 1)

    def test_register_run_rejects_date_change(self) -> None:
        self._register()
        with self.assertRaises(ReceiptConflictError):
            self.service.register_run(PIPELINE, {
                "run_id": "run-1", "batch_date": "2026-11-19",
            })

    def test_receipt_requires_registered_run(self) -> None:
        with self.assertRaises(RunNotFoundError):
            self.service.ingest_receipt(PIPELINE, "missing", self._gate())

    def test_partial_then_complete_report(self) -> None:
        self._register()
        partial = self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        self.assertEqual(partial["report"]["state"], "partial")
        self.assertEqual(
            partial["report"]["missing_receipts"], ["partition_progress"]
        )
        self.assertFalse(partial["report"]["current"]["progress"]["available"])

        complete = self.service.ingest_receipt(PIPELINE, "run-1", self._partitions())
        self.assertEqual(complete["report"]["state"], "complete")
        self.assertEqual(complete["report"]["current"]["quality"]["bad_row_rate"], 0.01)
        self.assertEqual(complete["report"]["current"]["progress"]["completion_pct"], 100.0)

    def test_duplicate_receipt_does_not_create_revision(self) -> None:
        self._register()
        first = self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        second = self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        self.assertEqual(first["receipt"]["revision"], 1)
        self.assertEqual(second["receipt"]["revision"], 1)
        self.assertTrue(second["receipt"]["deduplicated"])

    def test_invalid_receipt_leaves_no_trace(self) -> None:
        self._register()
        bad = {
            "receipt_type": "partition_progress",
            "payload": {"partitions": [
                {"partition": "2026-11-17/p0", "total_rows": 1, "failed_rows": 0},
            ]},
        }
        with self.assertRaises(ReceiptConflictError):
            self.service.ingest_receipt(PIPELINE, "run-1", bad)
        report = self.service.get_run_report(PIPELINE, "run-1")
        self.assertEqual(report["received_receipts"], [])

    def test_cannot_confirm_incomplete_report(self) -> None:
        self._register()
        self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        with self.assertRaises(ReceiptConflictError):
            self.service.confirm_report(PIPELINE, "run-1")

    def test_confirm_is_idempotent_and_snapshot_survives_refresh(self) -> None:
        self._register()
        self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        self.service.ingest_receipt(PIPELINE, "run-1", self._partitions())
        first = self.service.confirm_report(PIPELINE, "run-1")
        second = self.service.confirm_report(PIPELINE, "run-1")
        self.assertEqual(first["report_id"], second["report_id"])

        # 补入更多分区后，当前视图变化，但已确认快照不被覆盖。
        self.service.ingest_receipt(PIPELINE, "run-1", self._partitions(expected=3))
        current = self.service.get_run_report(PIPELINE, "run-1")
        self.assertEqual(current["current"]["state"], "partial")
        self.assertEqual(current["last_confirmed_scope"], "same_run")
        confirmed = self.service.get_confirmed_report(PIPELINE, "run-1")
        self.assertEqual(confirmed["report"]["quality"]["total_rows"], 1000)
        self.assertEqual(confirmed["report"]["progress"]["expected_partitions"], 2)

    def test_confirmed_missing_reports_404(self) -> None:
        self._register()
        with self.assertRaises(ReportNotConfirmedError):
            self.service.get_confirmed_report(PIPELINE, "run-1")

    def test_previous_run_scope_is_labelled(self) -> None:
        self._register("run-1", "2026-11-18")
        self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        self.service.ingest_receipt(PIPELINE, "run-1", {
            "receipt_type": "partition_progress",
            "payload": {"partitions": [
                {"partition": "2026-11-18/p0", "total_rows": 1000, "failed_rows": 10},
            ]},
        })
        self.service.confirm_report(PIPELINE, "run-1")

        self._register("run-2", "2026-11-19")
        self.service.ingest_receipt(PIPELINE, "run-2", {
            "receipt_type": "quality_gate",
            "payload": {"total_rows": 500, "passed_rows": 490, "failed_rows": 10},
        })
        report = self.service.get_run_report(PIPELINE, "run-2")
        self.assertEqual(report["last_confirmed_scope"], "previous_run")
        self.assertFalse(report["has_confirmed_report"])
        self.assertEqual(report["last_confirmed"]["run_id"], "run-1")

    def test_restart_does_not_duplicate_runs(self) -> None:
        self._register()
        self.service.ingest_receipt(PIPELINE, "run-1", self._gate())
        reopened = RunReportService(self.service.db.path)
        reopened.register_run(PIPELINE, {
            "run_id": "run-1", "batch_date": "2026-11-18",
        })
        self.assertEqual(len(reopened.list_runs(PIPELINE)), 1)
        self.assertEqual(
            reopened.get_run_report(PIPELINE, "run-1")["received_receipts"],
            ["quality_gate"],
        )

    def test_resolve_pipeline_rejects_wrong_owner(self) -> None:
        self._register()
        self.assertEqual(self.service.resolve_pipeline("run-1"), PIPELINE)
        with self.assertRaises(RunNotFoundError):
            self.service.resolve_pipeline("run-1", "另一条管线")


if __name__ == "__main__":
    unittest.main()
