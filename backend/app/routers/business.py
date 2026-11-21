"""业务编排API：跨模块的ETL编排、质量门禁、回刷、Schema变更等操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.engine.alert_matcher import AlertLevel, AlertRule
from app.engine.backfill_plan import BackfillEngine
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord, WatermarkStatus
from app.orchestrator import ETLOrchestrator

router = APIRouter(prefix="/api/business", tags=["业务编排"])

_orchestrator = ETLOrchestrator()


# ---- 管线编排 ----

@router.post("/pipelines/orchestrate")
def orchestrate_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    """为管线执行完整的DAG拓扑验证和血缘登记。

    body:
      pipeline_id, pipeline_name, source_table, target_table,
      stages: [{"name": "extract", "nodes": [{"id": "n1", "name": "读取"}]}, ...]
    """
    try:
        return _orchestrator.create_pipeline_with_full_orchestration(
            pipeline_id=body["pipeline_id"],
            pipeline_name=body["pipeline_name"],
            source_table=body["source_table"],
            target_table=body["target_table"],
            stages=body.get("stages", []),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {e}")


# ---- 质量门禁 ----

@router.post("/quality/gate")
def run_quality_gate(body: dict[str, Any]) -> dict[str, Any]:
    """对管线数据执行质量门禁。

    body:
      pipeline_id, rules: [{"rule_id", "rule_name", "target_table", "check_field",
                             "check_type", "threshold", "level", "extra": {}}],
      sample_data: [dict]
    """
    pipeline_id = body["pipeline_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
        )
        for r in body.get("rules", [])
    ]
    result = _orchestrator.run_quality_gate(
        pipeline_id, rules, body.get("sample_data", []),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message}
            for a in result.triggered_alerts
        ],
    }


# ---- 水位与回刷 ----

@router.post("/watermark/check")
def check_watermark(body: dict[str, Any]) -> dict[str, Any]:
    """检查水位并判断是否需要回刷。

    body:
      pipeline_id, partition_field, current_watermark, new_watermark,
      scheduled_time (ISO), backfill_threshold (可选)
    """
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=body["pipeline_id"],
        partition_field=body["partition_field"],
        current_watermark=body["current_watermark"],
        scheduled_time=datetime.fromisoformat(body["scheduled_time"]),
        status=WatermarkStatus(body.get("status", "正常")),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    return _orchestrator.check_watermark_and_backfill(
        record,
        body["new_watermark"],
        backfill_threshold=body.get("backfill_threshold", 3),
    )


# ---- 回刷计划 ----

@router.post("/backfill/plan")
def generate_backfill_plan(body: dict[str, Any]) -> dict[str, Any]:
    """生成回刷补偿方案。

    body:
      pipeline_id, start_partition, end_partition, parallelism, reason
    """
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id=body["pipeline_id"],
        start_partition=body["start_partition"],
        end_partition=body["end_partition"],
        parallelism=int(body.get("parallelism", 4)),
        reason=body.get("reason", ""),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    # 分配资源
    alloc = _orchestrator.allocate_for_pipeline(
        body["pipeline_id"],
        node_count=plan.total_partitions,
        estimated_memory_gb=plan.total_partitions * 0.5,
    )

    return {
        "plan": {
            "pipeline_id": plan.pipeline_id,
            "task_name": plan.task_name,
            "reason": plan.reason,
            "total_partitions": plan.total_partitions,
            "batch_count": len(plan.batches),
            "parallelism": plan.parallelism,
            "estimated_completion": plan.estimated_completion,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "partitions": b.range.partitions,
                    "count": b.range.count,
                }
                for b in plan.batches
            ],
        },
        "resource_allocation": alloc["allocation"],
    }


# ---- Schema变更评估 ----

@router.post("/schema/evaluate")
def evaluate_schema_change(body: dict[str, Any]) -> dict[str, Any]:
    """评估Schema变更的兼容性和影响。

    body:
      change_id, table_name,
      before_columns: [{"name", "data_type", "nullable", "is_partition"}],
      after_columns: [{"name", "data_type", "nullable", "is_partition"}],
      pipeline_deps: {table_name: [pipeline_ids]}
    """
    from app.engine.schema_evolution import SchemaColumn, SchemaDefinition

    before = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("before_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("before_columns", []) if c.get("is_partition")
        ],
    )
    after = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("after_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("after_columns", []) if c.get("is_partition")
        ],
    )
    result = _orchestrator.evaluate_schema_change(
        change_id=body.get("change_id", "auto"),
        before=before,
        after=after,
        pipeline_deps=body.get("pipeline_deps", {}),
    )
    return {
        "change_id": result.change.change_id,
        "table": result.change.target_table,
        "compatibility": result.change.compatibility.value,
        "changes": result.change.detail.get("changes", []),
        "affected_pipelines": result.change.affected_pipelines,
        "rollback_plan": result.change.rollback_plan,
        "migration_recommendation": result.migration_recommendation,
        "walk_pipelines": result.affected_pipelines,
    }


# ---- 血缘影响分析 ----

@router.get("/lineage/impact/{node_id}")
def analyze_lineage_impact(node_id: str) -> dict[str, Any]:
    """分析数据血缘影响：上游溯源 + 下游影响。"""
    result = _orchestrator.lineage.analyze_impact(node_id)
    return {
        "target_node": result.target_node,
        "upstream_count": len(result.upstream_nodes),
        "downstream_count": len(result.downstream_nodes),
        "affected_pipelines": result.affected_pipelines,
        "lineage_depth": result.lineage_depth,
        "has_cycle": result.has_cycle,
        "cycle_path": result.cycle_path,
    }


# ---- 资源健康 ----

@router.get("/resources/health")
def cluster_health() -> dict[str, Any]:
    """计算资源集群健康状态。"""
    return _orchestrator.balancer.cluster_health()


# ---- 告警批量评估 ----

@router.post("/alerts/evaluate")
def evaluate_alerts(body: dict[str, Any]) -> dict[str, Any]:
    """批量评估告警规则。

    body:
      rules: [{"rule_id", "rule_name", "monitor_target", "condition", "level"}],
      metrics: dict
    """
    rules = [
        AlertRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            monitor_target=r["monitor_target"],
            condition=r["condition"],
            level=AlertLevel(r.get("level", "WARN")),
            channels=r.get("channels", ["email"]),
        )
        for r in body.get("rules", [])
    ]
    matcher = _orchestrator.alert
    results = matcher.match_batch(rules, body.get("metrics", {}))

    return {
        "total_rules": len(rules),
        "triggered": sum(1 for r in results if r.matched),
        "results": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "matched": r.matched,
                "level": r.level.value,
                "message": r.message,
            }
            for r in results
        ],
    }


# ---- DAG拓扑分析 ----

@router.post("/dag/analyze")
def analyze_dag(body: dict[str, Any]) -> dict[str, Any]:
    """分析DAG拓扑结构。

    body:
      nodes: [{"id", "name", "weight"}],
      edges: [{"from", "to"}]
    """
    from app.engine.dag_topology import DagNode, DagTopology

    topo = DagTopology()
    for n in body.get("nodes", []):
        topo.add_node(DagNode(
            node_id=n["id"], name=n.get("name", n["id"]),
            weight=float(n.get("weight", 1.0)),
        ))
    for e in body.get("edges", []):
        topo.add_edge(e["from"], e["to"])

    result = topo.solve()
    return {
        "node_count": topo.node_count,
        "has_cycle": result.has_cycle,
        "cycle_nodes": result.cycle_nodes,
        "topology_order": result.order,
        "critical_path": result.critical_path,
        "critical_length": result.critical_length,
        "node_depth": result.node_depth,
    }


# ---- 综合管线运行报告 ----

@router.get("/pipelines/{pipeline_id}/run-report")
def pipeline_run_report(
    pipeline_id: str,
    run_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """获取管线某次实际运行的综合报告。

    报告只从已登记的回执（质量门禁、分区进度）计算，不再使用固定样本
    或“当天”的模拟进度。指定 run_id 时会先核验该运行编号确实属于本管线；
    未指定时返回该管线最近一次运行的报告，并在响应中明确 run_id。
    """
    from app.services.run_report import RunNotFoundError, run_report_service

    if run_id is not None:
        try:
            pipeline_id = run_report_service.resolve_pipeline(run_id, pipeline_id)
            return run_report_service.get_run_report(pipeline_id, run_id)
        except RunNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    runs = run_report_service.list_runs(pipeline_id=pipeline_id)
    if not runs:
        raise HTTPException(
            status_code=404,
            detail=f"管线 {pipeline_id} 还没有登记实际运行，无法生成报告",
        )
    return run_report_service.get_run_report(pipeline_id, runs[0]["run_id"])