"""综合运行报告路由：按运行编号登记实际回执、计算并发布报告。

接口分组：

- ``POST /api/runs``                                  登记一次运行（幂等）
- ``GET  /api/runs``                                  列出已登记运行及其回执齐全度
- ``POST /api/runs/{run_id}/receipts``                按运行编号提交实际回执
- ``GET  /api/runs/{run_id}/report``                  当前综合报告（含已收到哪些回执）
- ``POST /api/runs/{run_id}/report/confirm``          发布完整报告快照
- ``GET  /api/runs/{run_id}/report/confirmed``        读取已确认报告（可按 report_id 回看）
- ``GET  /api/pipelines/{pipeline_id}/runs/{run_id}`` 管线维度查看同一运行
- ``GET  /api/pipelines/{pipeline_id}/run-report``    兼容旧入口：必须带 run_id
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.engine.run_report import (
    ReceiptConflictError,
    ReceiptValidationError,
)
from app.services.run_report import (
    ReportNotConfirmedError,
    RunNotFoundError,
    run_report_service,
)

router = APIRouter(prefix="/api", tags=["运行报告"])


def _handle(call):
    try:
        return call()
    except RunNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ReportNotConfirmedError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ReceiptConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ReceiptValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/runs")
def register_run(body: dict[str, Any]) -> dict[str, Any]:
    """登记一次实际运行。

    body: {pipeline_id, run_id, batch_date, expected_partitions?}
    同一 (pipeline_id, run_id) 重复登记不会多记一条运行。
    """
    pipeline_id = body.get("pipeline_id")
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="缺少 pipeline_id")
    return _handle(lambda: run_report_service.register_run(str(pipeline_id), body))


@router.get("/runs")
def list_runs(
    pipeline_id: str | None = Query(default=None),
    batch_date: str | None = Query(default=None),
) -> dict[str, Any]:
    """列出已登记运行：当前已收到哪些回执、还缺哪些。"""
    items = run_report_service.list_runs(pipeline_id=pipeline_id, batch_date=batch_date)
    return {"items": items, "total": len(items)}


@router.post("/runs/{run_id}/receipts")
def ingest_receipt(run_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """提交实际运行回执。

    body: {pipeline_id, receipt_type, payload, batch_date?, expected_partitions?}
    receipt_type: quality_gate | partition_progress
    返回体里同时给出最新综合报告，方便值班立即复核。
    """
    pipeline_id = body.get("pipeline_id")
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="缺少 pipeline_id")
    return _handle(
        lambda: run_report_service.ingest_receipt(str(pipeline_id), run_id, body)
    )


@router.get("/runs/{run_id}/report")
def get_run_report(
    run_id: str,
    pipeline_id: str | None = Query(default=None, description="管线标识；省略时按运行编号反查"),
) -> dict[str, Any]:
    """当前综合报告：只从已确认的回执事实计算，并标注回执到达情况。"""
    def call():
        pipeline = run_report_service.resolve_pipeline(run_id, pipeline_id)
        return run_report_service.get_run_report(pipeline, run_id)
    return _handle(call)


@router.post("/runs/{run_id}/report/confirm")
def confirm_run_report(run_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """发布综合报告快照；只有完整、同轮的报告允许发布。"""
    pipeline_id = body.get("pipeline_id")
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="缺少 pipeline_id")
    return _handle(lambda: run_report_service.confirm_report(str(pipeline_id), run_id))


@router.get("/runs/{run_id}/report/confirmed")
def get_confirmed_report(
    run_id: str,
    pipeline_id: str | None = Query(default=None, description="管线标识；省略时按运行编号反查"),
    report_id: int | None = Query(default=None),
) -> dict[str, Any]:
    """读取数据库里已确认的报告；不带 report_id 时返回最新一份。"""
    def call():
        pipeline = run_report_service.resolve_pipeline(run_id, pipeline_id)
        return run_report_service.get_confirmed_report(pipeline, run_id, report_id)
    return _handle(call)


@router.get("/pipelines/{pipeline_id}/runs/{run_id}")
def pipeline_run(
    pipeline_id: str, run_id: str
) -> dict[str, Any]:
    """管线维度复核单次运行：当前事实 + 上次完整结果。"""
    return _handle(lambda: run_report_service.get_run_report(pipeline_id, run_id))


@router.get("/pipelines/{pipeline_id}/runs")
def pipeline_runs(pipeline_id: str) -> dict[str, Any]:
    """某条管线下的全部实际运行。"""
    items = run_report_service.list_runs(pipeline_id=pipeline_id)
    return {"pipeline_id": pipeline_id, "items": items, "total": len(items)}
