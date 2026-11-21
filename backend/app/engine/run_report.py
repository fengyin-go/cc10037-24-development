"""运行回执归一化与综合报告计算引擎。

一次管线运行会先后收到多份实际回执：

- ``quality_gate``：质量门禁执行结果（引用质量执行批次记录，含逐规则通过/失败行数）
- ``partition_progress``：分区校验进度（逐个分区的实际通过/失败行数）

本模块只做纯计算：校验回执内部计数、确认所有证据属于同一运行日期，
再从实际行数、分区完成度和规则阈值推导质量结论、进度和告警摘要。
真正的落库与事务边界留在 ``app.services.run_report``。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

QUALITY_GATE = "quality_gate"
PARTITION_PROGRESS = "partition_progress"
RECEIPT_TYPES: tuple[str, ...] = (QUALITY_GATE, PARTITION_PROGRESS)

RECEIPT_ALIASES: dict[str, str] = {
    "quality_gate": QUALITY_GATE,
    "quality": QUALITY_GATE,
    "gate": QUALITY_GATE,
    "质量门禁": QUALITY_GATE,
    "partition_progress": PARTITION_PROGRESS,
    "partition": PARTITION_PROGRESS,
    "progress": PARTITION_PROGRESS,
    "分区进度": PARTITION_PROGRESS,
}

RECEIPT_LABELS = {
    QUALITY_GATE: "质量门禁回执",
    PARTITION_PROGRESS: "分区进度回执",
}

DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_RUNNING = "running"

_STATUS_ALIASES = {
    "success": STATUS_SUCCESS,
    "succeeded": STATUS_SUCCESS,
    "done": STATUS_SUCCESS,
    "completed": STATUS_SUCCESS,
    "complete": STATUS_SUCCESS,
    "passed": STATUS_SUCCESS,
    "成功": STATUS_SUCCESS,
    "已完成": STATUS_SUCCESS,
    "通过": STATUS_SUCCESS,
    "failed": STATUS_FAILED,
    "failure": STATUS_FAILED,
    "error": STATUS_FAILED,
    "失败": STATUS_FAILED,
    "running": STATUS_RUNNING,
    "processing": STATUS_RUNNING,
    "进行中": STATUS_RUNNING,
    "校验中": STATUS_RUNNING,
}


class ReceiptValidationError(ValueError):
    """回执内容不合法；路由层会映射为 400。"""


class ReceiptConflictError(ValueError):
    """回执与已登记运行事实冲突（日期、管线、计数不一致）；路由层映射为 409。"""


def normalize_receipt_type(value: Any) -> str:
    """把外部传入的回执类型归一化为内部常量。"""
    key = str(value or "").strip().casefold()
    receipt_type = RECEIPT_ALIASES.get(key)
    if receipt_type is None:
        raise ReceiptValidationError(
            f"未知回执类型「{value}」，仅支持 {'、'.join(RECEIPT_TYPES)}"
        )
    return receipt_type


def _as_int(value: Any, field_name: str, *, default: int | None = None) -> int:
    if value is None or str(value).strip() == "":
        if default is not None:
            return default
        raise ReceiptValidationError(f"缺少字段 {field_name}")
    if isinstance(value, bool):
        raise ReceiptValidationError(f"字段 {field_name} 必须为整数")
    try:
        number = int(float(str(value).replace(",", "")))
    except (TypeError, ValueError) as exc:
        raise ReceiptValidationError(f"字段 {field_name} 必须为整数") from exc
    if number < 0:
        raise ReceiptValidationError(f"字段 {field_name} 不能为负数")
    return number


def _as_float(value: Any, field_name: str, *, default: float) -> float:
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ReceiptValidationError(f"字段 {field_name} 必须为数字") from exc


def _date_in_text(value: Any) -> str | None:
    if not value:
        return None
    match = DATE_PATTERN.search(str(value))
    return match.group(0) if match else None


@dataclass
class RowCounts:
    """实际行数。三个数字必须能相互对上，避免报告使用拼凑数据。"""

    total_rows: int
    passed_rows: int
    failed_rows: int

    def __post_init__(self) -> None:
        if self.passed_rows + self.failed_rows != self.total_rows:
            raise ReceiptValidationError(
                "行数不自洽：通过行数 + 失败行数 必须等于总行数"
                f"（{self.passed_rows} + {self.failed_rows} != {self.total_rows}）"
            )

    @property
    def bad_row_rate(self) -> float:
        if self.total_rows <= 0:
            return 0.0
        return round(self.failed_rows / self.total_rows, 6)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "bad_row_rate": self.bad_row_rate,
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any], *, prefix: str = "") -> "RowCounts":
        total = payload.get(f"{prefix}total_rows")
        passed = payload.get(f"{prefix}passed_rows")
        failed = payload.get(f"{prefix}failed_rows")

        if total is None and passed is None and failed is None:
            raise ReceiptValidationError(
                f"缺少 {prefix or ''}总行数/通过行数/失败行数"
            )

        if total is None:
            passed_num = _as_int(passed, "passed_rows")
            failed_num = _as_int(failed, "failed_rows")
            total_num = passed_num + failed_num
        else:
            total_num = _as_int(total, "total_rows")
            if passed is None and failed is None:
                raise ReceiptValidationError("缺少通过行数或失败行数")
            if passed is None:
                failed_num = _as_int(failed, "failed_rows")
                passed_num = total_num - failed_num
            elif failed is None:
                passed_num = _as_int(passed, "passed_rows")
                failed_num = total_num - passed_num
            else:
                passed_num = _as_int(passed, "passed_rows")
                failed_num = _as_int(failed, "failed_rows")
        if passed_num < 0 or failed_num < 0:
            raise ReceiptValidationError("通过行数/失败行数不能大于总行数")
        return cls(total_rows=total_num, passed_rows=passed_num, failed_rows=failed_num)


@dataclass
class QualityRuleFact:
    """一条质量规则的执行事实。"""

    rule_id: str
    rule_name: str
    counts: RowCounts
    check_field: str = ""
    check_type: str = ""
    threshold: float = 0.01
    level: str = "WARN"
    validation_run_id: int | None = None

    @property
    def failed_ratio(self) -> float:
        return self.counts.bad_row_rate

    @property
    def threshold_exceeded(self) -> bool:
        return self.counts.total_rows > 0 and self.failed_ratio > self.threshold

    @property
    def blocked(self) -> bool:
        return self.level == "BLOCK" and self.threshold_exceeded

    def to_dict(self) -> dict[str, Any]:
        result = {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "check_field": self.check_field,
            "check_type": self.check_type,
            "threshold": self.threshold,
            "level": self.level,
            **self.counts.to_dict(),
            "failed_ratio": self.failed_ratio,
            "threshold_exceeded": self.threshold_exceeded,
            "blocked": self.blocked,
        }
        if self.validation_run_id is not None:
            result["validation_run_id"] = self.validation_run_id
        return result


@dataclass
class QualityGateFacts:
    """质量门禁回执归一化后的事实。"""

    aggregate: RowCounts | None
    rules: list[QualityRuleFact] = field(default_factory=list)
    decision: str = "RELEASE"
    overall_pass: bool = True
    computed_decision: str = "RELEASE"
    threshold: float = 0.01
    level: str = "WARN"
    validation_run_id: int | None = None
    validation_batch: str = ""
    target_table: str = ""
    ready: bool = True
    reported_decision: str | None = None
    validation_status: str = ""
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": True,
            "ready": self.ready,
            "decision": self.decision,
            "overall_pass": self.overall_pass,
            "computed_decision": self.computed_decision,
            "threshold": self.threshold,
            "level": self.level,
            "validation_run_id": self.validation_run_id,
            "validation_batch": self.validation_batch,
            "target_table": self.target_table,
            "aggregate": self.aggregate.to_dict() if self.aggregate else None,
            "rules": [rule.to_dict() for rule in self.rules],
            "reported_decision": self.reported_decision,
            "validation_status": self.validation_status,
            "warnings": self.warnings,
        }


@dataclass
class PartitionFact:
    """一个分区的实际校验结果。"""

    partition_key: str
    batch_date: str
    counts: RowCounts
    status: str = STATUS_SUCCESS
    validation_run_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition": self.partition_key,
            "batch_date": self.batch_date,
            "status": self.status,
            **self.counts.to_dict(),
            "validation_run_id": self.validation_run_id,
        }


@dataclass
class PartitionProgressFacts:
    """分区进度回执归一化后的事实。"""

    partitions: list[PartitionFact]
    expected_partitions: int
    completed_partitions: int
    failed_partitions: int
    aggregate: RowCounts
    complete: bool

    @property
    def completion_pct(self) -> float:
        if self.expected_partitions <= 0:
            return 0.0
        return round(self.completed_partitions / self.expected_partitions * 100, 2)

    @property
    def success_pct(self) -> float:
        if self.expected_partitions <= 0:
            return 0.0
        successful = sum(1 for p in self.partitions if p.status == STATUS_SUCCESS)
        return round(successful / self.expected_partitions * 100, 2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": True,
            "expected_partitions": self.expected_partitions,
            "received_partitions": len(self.partitions),
            "completed_partitions": self.completed_partitions,
            "failed_partitions": self.failed_partitions,
            "completion_pct": self.completion_pct,
            "success_pct": self.success_pct,
            "complete": self.complete,
            **self.aggregate.to_dict(),
            "partitions": [p.to_dict() for p in self.partitions],
        }


# ---- 回执归一化 ----


def _normalize_rule(raw: dict[str, Any]) -> QualityRuleFact:
    if not isinstance(raw, dict):
        raise ReceiptValidationError("质量规则回执必须是对象列表")
    rule_id = str(raw.get("rule_id") or raw.get("rule") or "").strip()
    if not rule_id:
        raise ReceiptValidationError("质量规则缺少 rule_id")
    level = str(raw.get("level") or raw.get("校验级别") or "WARN").upper()
    if level not in {"WARN", "BLOCK", "FIX"}:
        raise ReceiptValidationError(f"规则 {rule_id} 的 level 只能是 WARN/BLOCK/FIX")
    return QualityRuleFact(
        rule_id=rule_id,
        rule_name=str(raw.get("rule_name") or raw.get("规则名称") or rule_id),
        counts=RowCounts.from_mapping(raw),
        check_field=str(raw.get("check_field") or raw.get("校验字段") or ""),
        check_type=str(raw.get("check_type") or raw.get("校验类型") or ""),
        threshold=_as_float(
            raw.get("threshold", raw.get("阈值设定")), "threshold", default=0.01
        ),
        level=level,
        validation_run_id=(
            _as_int(raw["validation_run_id"], "validation_run_id")
            if raw.get("validation_run_id") is not None else None
        ),
    )


def normalize_quality_gate(payload: dict[str, Any]) -> QualityGateFacts:
    """校验并归一化质量门禁回执。

    行数由回执中的实际执行事实提供；若引用 ``validation_run_id``，
    服务层会再与质量执行记录交叉核对。
    """
    if not isinstance(payload, dict):
        raise ReceiptValidationError("质量门禁回执 payload 必须是对象")

    raw_rules = payload.get("rules") or []
    if not isinstance(raw_rules, list):
        raise ReceiptValidationError("rules 必须是数组")
    rules = [_normalize_rule(rule) for rule in raw_rules]

    aggregate: RowCounts | None = None
    if any(key in payload for key in ("total_rows", "passed_rows", "failed_rows")):
        aggregate = RowCounts.from_mapping(payload)
    elif len(rules) == 1:
        aggregate = rules[0].counts

    if aggregate is None and not rules:
        raise ReceiptValidationError(
            "质量门禁回执缺少实际行数：请提供 total_rows/passed_rows/failed_rows 或 rules"
        )

    threshold = _as_float(payload.get("threshold"), "threshold", default=0.01)
    level = str(payload.get("level") or "WARN").upper()
    if level not in {"WARN", "BLOCK", "FIX"}:
        raise ReceiptValidationError("质量门禁 level 只能是 WARN/BLOCK/FIX")

    blocked_by_rules = any(rule.blocked for rule in rules)
    if rules:
        computed_decision = "BLOCKED" if blocked_by_rules else "RELEASE"
    elif aggregate is not None and aggregate.total_rows > 0:
        computed_decision = (
            "BLOCKED"
            if level == "BLOCK" and aggregate.bad_row_rate > threshold
            else "RELEASE"
        )
    else:
        computed_decision = "PENDING"

    # 结论优先级：回执里明确的门禁结论 → 关联质量执行批次的结论 →
    # 按实际行数和阈值推算。所有结论都会同时给出，方便值班复核差异。
    reported_decision = payload.get("decision") or payload.get("gate_passed")
    if isinstance(reported_decision, bool):
        reported_decision = "RELEASE" if reported_decision else "BLOCKED"
    normalized_reported: str | None = None
    if reported_decision is not None:
        text = str(reported_decision).strip().upper()
        if text in {"RELEASE", "PASS", "PASSED", "通过"}:
            normalized_reported = "RELEASE"
        elif text in {"BLOCKED", "BLOCK", "FAIL", "FAILED", "阻断", "未通过"}:
            normalized_reported = "BLOCKED"
        elif text not in {"", "PENDING"}:
            raise ReceiptValidationError(
                f"质量门禁结论 {reported_decision} 无法识别，应为 RELEASE/BLOCKED"
            )

    validation_status = str(payload.get("validation_status") or "").strip()
    status_decision: str | None = None
    if validation_status in {"已通过"}:
        status_decision = "RELEASE"
    elif validation_status in {"未通过", "已阻断"}:
        status_decision = "BLOCKED"

    # 结论优先级：回执里明确的门禁结论 → 阻断级规则命中 → 关联质量执行批次
    # 的状态 → 按实际行数和阈值推算。阻断级规则命中时不允许被较旧的批次状态
    # 覆盖，但会在 warnings 里保留差异供值班复核。
    decision = normalized_reported or (
        "BLOCKED" if blocked_by_rules else (status_decision or computed_decision)
    )

    warnings: list[str] = []
    if computed_decision != "PENDING" and decision != computed_decision:
        warnings.append(
            f"实际执行结论为 {decision}，按行数与阈值推算为 {computed_decision}，"
            "报告保留实际执行结论并同时展示推算值"
        )
    if status_decision and blocked_by_rules and decision == "BLOCKED" and status_decision == "RELEASE":
        warnings.append(
            f"质量执行记录状态为 {validation_status}，但存在阻断级规则失败，报告按阻断处理"
        )

    return QualityGateFacts(
        aggregate=aggregate,
        rules=rules,
        decision=decision,
        overall_pass=decision == "RELEASE",
        computed_decision=computed_decision,
        threshold=threshold,
        level=level,
        validation_run_id=(
            _as_int(payload["validation_run_id"], "validation_run_id")
            if payload.get("validation_run_id") is not None else None
        ),
        validation_batch=str(payload.get("validation_batch") or ""),
        target_table=str(payload.get("target_table") or ""),
        ready=bool(aggregate and aggregate.total_rows > 0),
        reported_decision=normalized_reported or status_decision,
        validation_status=validation_status,
        warnings=warnings,
    )


def normalize_partition_progress(
    payload: dict[str, Any], *, batch_date: str
) -> PartitionProgressFacts:
    """校验并归一化分区进度回执，拒绝跨日期拼接。"""
    if not isinstance(payload, dict):
        raise ReceiptValidationError("分区进度回执 payload 必须是对象")
    raw_partitions = payload.get("partitions")
    if not isinstance(raw_partitions, list) or not raw_partitions:
        raise ReceiptValidationError("分区进度回执缺少 partitions 数组")

    partitions: list[PartitionFact] = []
    for index, raw in enumerate(raw_partitions):
        if not isinstance(raw, dict):
            raise ReceiptValidationError("partitions 的每一项必须是对象")
        key = str(
            raw.get("partition")
            or raw.get("partition_key")
            or raw.get("key")
            or raw.get("name")
            or ""
        ).strip()
        if not key:
            raise ReceiptValidationError(f"第 {index + 1} 个分区缺少 partition 标识")

        raw_date = raw.get("batch_date") or raw.get("partition_date") or raw.get("date")
        partition_date = _date_in_text(raw_date) or _date_in_text(key)
        if partition_date is None:
            partition_date = batch_date
        if partition_date != batch_date:
            raise ReceiptConflictError(
                f"分区 {key} 的日期 {partition_date} 与运行日期 {batch_date} 不一致，"
                "不能拼接不同日期的证据"
            )

        status = _STATUS_ALIASES.get(
            str(raw.get("status") or raw.get("state") or "").strip().casefold(),
            STATUS_SUCCESS,
        )
        counts = RowCounts.from_mapping(raw)
        partitions.append(PartitionFact(
            partition_key=key,
            batch_date=partition_date,
            counts=counts,
            status=status,
            validation_run_id=(
                _as_int(raw["validation_run_id"], "validation_run_id")
                if raw.get("validation_run_id") is not None else None
            ),
        ))

    completed = sum(
        1 for p in partitions if p.status in (STATUS_SUCCESS, STATUS_FAILED)
    )
    failed = sum(1 for p in partitions if p.status == STATUS_FAILED)
    expected = _as_int(
        payload.get("expected_partitions"), "expected_partitions",
        default=len(partitions),
    )
    if expected < completed:
        raise ReceiptValidationError(
            f"expected_partitions={expected} 小于已完成分区数 {completed}"
        )

    totals = RowCounts(
        total_rows=sum(p.counts.total_rows for p in partitions),
        passed_rows=sum(p.counts.passed_rows for p in partitions),
        failed_rows=sum(p.counts.failed_rows for p in partitions),
    )
    return PartitionProgressFacts(
        partitions=partitions,
        expected_partitions=expected,
        completed_partitions=completed,
        failed_partitions=failed,
        aggregate=totals,
        complete=expected > 0 and completed >= expected,
    )


# ---- 综合报告计算 ----


def _counts_equal(left: RowCounts, right: RowCounts) -> bool:
    return (
        left.total_rows == right.total_rows
        and left.passed_rows == right.passed_rows
        and left.failed_rows == right.failed_rows
    )


def _alert_level_for_rule(rule: QualityRuleFact) -> str:
    if rule.level == "BLOCK":
        return "CRITICAL"
    if rule.level == "WARN":
        return "WARN"
    return "INFO"


def _highest_level(alerts: list[dict[str, Any]]) -> str:
    order = {"INFO": 0, "WARN": 1, "CRITICAL": 2, "FATAL": 3}
    if not alerts:
        return "NONE"
    return max(alerts, key=lambda item: order.get(item["level"], 0))["level"]


def compute_run_report(
    *,
    run: dict[str, Any],
    quality: QualityGateFacts | None,
    partitions: PartitionProgressFacts | None,
    receipt_meta: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """从已确认的运行事实计算综合报告数据。

    ``run`` 至少包含 run_id / pipeline_id / batch_date。
    返回的状态只有 ``waiting`` / ``partial`` / ``complete`` / ``inconsistent``；
    ``confirmed`` 是数据库层的发布状态，不在此处伪造。
    """
    run_id = str(run["run_id"])
    pipeline_id = str(run["pipeline_id"])
    batch_date = str(run["batch_date"])
    receipt_meta = receipt_meta or {}

    received = [
        receipt_type
        for receipt_type, present in (
            (QUALITY_GATE, quality is not None),
            (PARTITION_PROGRESS, partitions is not None),
        )
        if present
    ]
    missing = [receipt_type for receipt_type in RECEIPT_TYPES if receipt_type not in received]

    issues: list[str] = []
    inconsistent = False

    # 质量行数：门禁汇总 vs 分区证据逐分区汇总。
    gate_counts = quality.aggregate if quality and quality.aggregate else None
    partition_counts = partitions.aggregate if partitions else None

    if gate_counts and partitions:
        if partitions.complete:
            if not _counts_equal(gate_counts, partition_counts):
                inconsistent = True
                issues.append(
                    "质量门禁与分区证据的行数不一致："
                    f"门禁 {gate_counts.total_rows} 行 / 分区 {partition_counts.total_rows} 行"
                )
        elif partition_counts.total_rows > gate_counts.total_rows:
            inconsistent = True
            issues.append("分区证据累计行数已超过质量门禁总行数，证据不属于同一轮")

    if quality is not None and not quality.ready:
        issues.append("质量执行批次尚未产出有效行数，质量结论待补全")
    if quality is not None:
        issues.extend(quality.warnings)
    if partitions is not None and not partitions.complete:
        issues.append(
            f"分区证据未齐：已完成 {partitions.completed_partitions}/"
            f"{partitions.expected_partitions} 个分区"
        )
    for receipt_type in missing:
        issues.append(f"缺少{RECEIPT_LABELS[receipt_type]}")

    # 质量摘要：分区证据齐了才用逐分区汇总（此时它覆盖整批数据）；
    # 分区还在同步时保留门禁的全量行数，避免中途把坏行率算小。
    row_counts: RowCounts | None = None
    quality_source = "none"
    if partitions is not None and partitions.complete and partitions.aggregate.total_rows > 0:
        row_counts = partitions.aggregate
        quality_source = "partition_progress"
        if gate_counts is not None:
            quality_source = "partition_progress+quality_gate"
    elif gate_counts is not None and gate_counts.total_rows > 0:
        row_counts = gate_counts
        quality_source = "quality_gate"
    elif partitions is not None and partitions.aggregate.total_rows > 0:
        row_counts = partitions.aggregate
        quality_source = "partition_progress(partial)"

    if quality is not None:
        decision = quality.decision
        overall_pass = quality.overall_pass
    elif row_counts is not None:
        failed = row_counts.bad_row_rate
        threshold = _as_float(run.get("threshold"), "threshold", default=0.01)
        overall_pass = failed <= threshold
        decision = "RELEASE" if overall_pass else "BLOCKED"
    else:
        decision = "PENDING"
        overall_pass = False

    quality_summary: dict[str, Any] = {
        "available": row_counts is not None and (quality is None or quality.ready),
        "ready": quality.ready if quality else row_counts is not None,
        "source": quality_source,
        "decision": decision,
        "overall_pass": overall_pass,
        "computed_decision": quality.computed_decision if quality else decision,
        "validation_run_id": quality.validation_run_id if quality else None,
        "validation_batch": quality.validation_batch if quality else "",
        "target_table": quality.target_table if quality else "",
        "validation_status": quality.validation_status if quality else "",
        "rules": [rule.to_dict() for rule in quality.rules] if quality else [],
        "warnings": list(quality.warnings) if quality else [],
    }
    if row_counts is not None:
        quality_summary.update(row_counts.to_dict())
    else:
        quality_summary.update({
            "total_rows": 0,
            "passed_rows": 0,
            "failed_rows": 0,
            "bad_row_rate": 0.0,
        })

    # 进度摘要：没有分区回执时只展示已登记的期望数，不伪造已完成量。
    if partitions is not None:
        progress = {
            "available": True,
            **partitions.to_dict(),
        }
    else:
        progress = {
            "available": False,
            "expected_partitions": run.get("expected_partitions") or 0,
            "received_partitions": 0,
            "completed_partitions": 0,
            "failed_partitions": 0,
            "completion_pct": 0.0,
            "success_pct": 0.0,
            "complete": False,
            "total_rows": 0,
            "passed_rows": 0,
            "failed_rows": 0,
            "bad_row_rate": 0.0,
            "partitions": [],
        }

    # 告警摘要全部来自实际事实：规则阈值、失败分区、质量阻断。
    alerts: list[dict[str, Any]] = []
    if quality is not None:
        for rule in quality.rules:
            if rule.threshold_exceeded:
                alerts.append({
                    "source": "quality_rule",
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "level": _alert_level_for_rule(rule),
                    "message": (
                        f"{rule.rule_name} 失败率 {rule.failed_ratio:.4%} "
                        f"超过阈值 {rule.threshold:.4%}"
                    ),
                })
        if not overall_pass and not any(
            alert["source"] == "quality_rule" and alert["level"] == "CRITICAL"
            for alert in alerts
        ):
            row_text = (
                f"坏行率 {row_counts.bad_row_rate:.4%}" if row_counts else "质量结论阻断"
            )
            alerts.append({
                "source": "quality_gate",
                "rule_id": "gate",
                "rule_name": "质量门禁",
                "level": "CRITICAL",
                "message": f"质量门禁阻断：{row_text}",
            })
    if partitions is not None:
        for fact in partitions.partitions:
            if fact.status == STATUS_FAILED:
                alerts.append({
                    "source": "partition",
                    "rule_id": fact.partition_key,
                    "rule_name": fact.partition_key,
                    "level": "CRITICAL",
                    "message": (
                        f"分区 {fact.partition_key} 校验失败："
                        f"{fact.counts.failed_rows}/{fact.counts.total_rows} 行未通过"
                    ),
                })
        if not partitions.complete:
            alerts.append({
                "source": "partition_progress",
                "rule_id": "progress",
                "rule_name": "分区进度",
                "level": "WARN",
                "message": (
                    f"分区证据未齐：{partitions.completed_partitions}/"
                    f"{partitions.expected_partitions}"
                ),
            })
    if quality is None and row_counts is not None and not overall_pass:
        alerts.append({
            "source": "quality_gate",
            "rule_id": "aggregate",
            "rule_name": "质量汇总",
            "level": "CRITICAL",
            "message": f"实际坏行率 {row_counts.bad_row_rate:.4%} 超过阈值",
        })

    by_level: dict[str, int] = {}
    for alert in alerts:
        by_level[alert["level"]] = by_level.get(alert["level"], 0) + 1
    alert_summary = {
        "total": len(alerts),
        "by_level": by_level,
        "highest_level": _highest_level(alerts),
        "items": alerts,
    }

    if inconsistent:
        state = "inconsistent"
    elif quality is not None and partitions is not None and partitions.complete and quality.ready:
        state = "complete"
    elif not received:
        state = "waiting"
    else:
        state = "partial"

    health = "healthy"
    if inconsistent or (quality_summary["available"] and not overall_pass) or any(
        alert["level"] == "CRITICAL" for alert in alerts
    ):
        health = "critical"
    elif state != "complete" or alerts:
        health = "degraded"

    same_round = not inconsistent and all(
        date_value == batch_date for date_value in (
            [p.batch_date for p in partitions.partitions] if partitions else []
        )
    )
    evidence = {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "batch_date": batch_date,
        "same_round": same_round,
        "received_receipts": received,
        "missing_receipts": missing,
        "partition_dates": sorted({p.batch_date for p in partitions.partitions})
        if partitions else [],
        "receipts": {
            receipt_type: receipt_meta.get(receipt_type, {})
            for receipt_type in received
        },
    }

    return {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "batch_date": batch_date,
        "state": state,
        "generated_at": None,  # 落库时由服务层补上，保持纯计算
        "quality": quality_summary,
        "progress": progress,
        "alerts": alert_summary,
        "alert_items": alerts,
        "health": health,
        "issues": issues,
        "evidence": evidence,
        "receipt_summary": {
            "received": received,
            "missing": missing,
            "labels": {
                receipt_type: RECEIPT_LABELS[receipt_type] for receipt_type in received
            },
        },
    }
