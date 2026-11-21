"""演示用的两个实际订单批次运行。

写入的是"实际回执"形状的数据，不是默认样本：

- ``ORD-RUN-20261118``：订单宽表 2026-11-18 批次，门禁 + 分区证据齐全，
  已确认发布，用于回看上次完整结果。
- ``ORD-RUN-20261117``：订单宽表 2026-11-17 批次，目前只到质量门禁回执，
  分区证据仍在同步，用于演示"回执先后到达"的中间态。

两个批次的行数都能和 ``validation_run`` 里的实际执行记录对上，
因此报告端算出的坏行率是真实比例，而不是模拟值。
"""

from __future__ import annotations

from typing import Any

from app.engine.run_report import PARTITION_PROGRESS, QUALITY_GATE
from app.services.run_report import RunReportService

PIPELINE = "ODS到DWD订单宽表ETL"

PASS_RUN_ID = "ORD-RUN-20261118"
PASS_BATCH_DATE = "2026-11-18"
FAIL_RUN_ID = "ORD-RUN-20261117"
FAIL_BATCH_DATE = "2026-11-17"

# 与 validation_run 第 1、4 条记录一致：总行/通过/失败逐项相等。
_PASS_GATE: dict[str, Any] = {
    "validation_run_id": 1,
    "threshold": 0.01,
    "level": "WARN",
    "rules": [
        {
            "rule_id": "rule-1",
            "rule_name": "订单主键非空检查",
            "check_field": "order_id",
            "check_type": "NOT_NULL",
            "total_rows": 3215048,
            "passed_rows": 3208937,
            "failed_rows": 6111,
            "threshold": 0.01,
            "level": "WARN",
        },
    ],
}

_PASS_PARTITIONS: dict[str, Any] = {
    "expected_partitions": 3,
    "partitions": [
        {"partition": "2026-11-18/p0", "total_rows": 1500000, "passed_rows": 1497500, "failed_rows": 2500},
        {"partition": "2026-11-18/p1", "total_rows": 1000000, "passed_rows": 998000, "failed_rows": 2000},
        {"partition": "2026-11-18/p2", "total_rows": 715048, "passed_rows": 713437, "failed_rows": 1611},
    ],
}

_FAIL_GATE: dict[str, Any] = {
    "validation_run_id": 4,
    "threshold": 0.001,
    "level": "BLOCK",
    "rules": [
        {
            "rule_id": "rule-1",
            "rule_name": "订单主键非空检查",
            "check_field": "order_id",
            "check_type": "NOT_NULL",
            "total_rows": 3100023,
            "passed_rows": 2978120,
            "failed_rows": 121903,
            "threshold": 0.001,
            "level": "BLOCK",
        },
    ],
}

_FAIL_PARTITIONS: dict[str, Any] = {
    "expected_partitions": 3,
    "partitions": [
        {"partition": "2026-11-17/p0", "total_rows": 1200000, "passed_rows": 1160000, "failed_rows": 40000},
        {"partition": "2026-11-17/p1", "total_rows": 1000000, "passed_rows": 950000, "failed_rows": 50000},
        {"partition": "2026-11-17/p2", "total_rows": 900023, "passed_rows": 868120, "failed_rows": 31903},
    ],
}


def seed_run_reports(service: RunReportService) -> dict[str, Any]:
    """只在库中还没有这两个批次时写入；重复启动不会多记运行。"""
    existing = {item["run_id"] for item in service.list_runs(pipeline_id=PIPELINE)}
    created: list[str] = []

    if PASS_RUN_ID not in existing:
        service.register_run(PIPELINE, {
            "run_id": PASS_RUN_ID,
            "batch_date": PASS_BATCH_DATE,
            "expected_partitions": 3,
        })
        service.ingest_receipt(PIPELINE, PASS_RUN_ID, {
            "receipt_type": QUALITY_GATE,
            "payload": _PASS_GATE,
        })
        service.ingest_receipt(PIPELINE, PASS_RUN_ID, {
            "receipt_type": PARTITION_PROGRESS,
            "payload": _PASS_PARTITIONS,
        })
        service.confirm_report(PIPELINE, PASS_RUN_ID)
        created.append(PASS_RUN_ID)

    if FAIL_RUN_ID not in existing:
        service.register_run(PIPELINE, {
            "run_id": FAIL_RUN_ID,
            "batch_date": FAIL_BATCH_DATE,
            "expected_partitions": 3,
        })
        # 只投递质量门禁；分区回执留给值班在分区证据同步后补入。
        service.ingest_receipt(PIPELINE, FAIL_RUN_ID, {
            "receipt_type": QUALITY_GATE,
            "payload": _FAIL_GATE,
        })
        created.append(FAIL_RUN_ID)

    return {"created": created, "pipeline_id": PIPELINE}
