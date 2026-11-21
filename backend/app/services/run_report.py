"""运行回执与综合报告持久化服务。

一次管线运行以 ``(pipeline_id, run_id)`` 为唯一键登记，回执按类型分版本
落库，综合报告按同一组键保存为不可变快照：

- ``pipeline_runs``：运行主记录，幂等登记，重复投递不会多记一条运行。
- ``run_receipts``：质量门禁 / 分区进度回执，保留每轮修订与内容指纹。
- ``run_reports``：已确认的综合报告快照，与当前回执解耦，后续补数据不会覆盖。

所有写入都在一个 SQLite 事务内完成，进程在写回执途中退出时不会留下
半条运行或半份回执；重启后继续投递即可回到一致状态。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from app.config import settings
from app.engine.run_report import (
    PARTITION_PROGRESS,
    QUALITY_GATE,
    RECEIPT_LABELS,
    RECEIPT_TYPES,
    PartitionProgressFacts,
    QualityGateFacts,
    ReceiptConflictError,
    ReceiptValidationError,
    RowCounts,
    compute_run_report,
    normalize_partition_progress,
    normalize_quality_gate,
    normalize_receipt_type,
)
from app.store import store

RUN_ID_PATTERN_MAX = 128


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class RunNotFoundError(LookupError):
    """运行编号未登记；路由层映射为 404。"""


class ReportNotConfirmedError(LookupError):
    """该运行还没有已确认的综合报告；路由层映射为 404。"""


class _Database:
    """极简 SQLite 访问层：每次操作独立连接，WAL 保证读写并发。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            with self._connect() as connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        pipeline_id TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        batch_date TEXT NOT NULL,
                        expected_partitions INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        confirmed_at TEXT,
                        PRIMARY KEY (pipeline_id, run_id)
                    );

                    CREATE TABLE IF NOT EXISTS run_receipts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        receipt_type TEXT NOT NULL,
                        revision INTEGER NOT NULL,
                        content_hash TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        batch_date TEXT NOT NULL,
                        received_at TEXT NOT NULL,
                        UNIQUE (pipeline_id, run_id, receipt_type, revision),
                        FOREIGN KEY (pipeline_id, run_id)
                            REFERENCES pipeline_runs (pipeline_id, run_id)
                            ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_run_receipts_latest
                        ON run_receipts (pipeline_id, run_id, receipt_type, revision DESC);

                    CREATE TABLE IF NOT EXISTS run_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        batch_date TEXT NOT NULL,
                        report_hash TEXT NOT NULL,
                        report_json TEXT NOT NULL,
                        confirmed_at TEXT NOT NULL,
                        UNIQUE (pipeline_id, run_id, report_hash),
                        FOREIGN KEY (pipeline_id, run_id)
                            REFERENCES pipeline_runs (pipeline_id, run_id)
                            ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_run_reports_latest
                        ON run_reports (pipeline_id, run_id, id DESC);
                    """
                )
            self._initialized = True

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 先拿写锁，异常整体回滚。"""
        self.initialize()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        self.initialize()
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()


class RunReportService:
    """运行回执入库与综合报告计算。"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db = _Database(db_path or settings.run_report_db)

    # ---- 运行登记 ----

    def register_run(self, pipeline_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """幂等登记一次运行；同键重复投递返回原记录，不新增。"""
        pipeline_id = self._require_pipeline(pipeline_id)
        run_id = self._require_run_id(payload.get("run_id") or payload.get("execution_id"))
        batch_date = self._require_batch_date(payload.get("batch_date"))
        expected = payload.get("expected_partitions")
        expected_partitions = self._as_expected(expected)

        with self.db.transaction() as connection:
            existing = self._select_run(connection, pipeline_id, run_id)
            if existing is not None:
                if existing["batch_date"] != batch_date:
                    raise ReceiptConflictError(
                        f"运行 {run_id} 已登记日期 {existing['batch_date']}，"
                        f"与本次 {batch_date} 不一致"
                    )
                if expected_partitions > existing["expected_partitions"]:
                    connection.execute(
                        "UPDATE pipeline_runs SET expected_partitions = ?, updated_at = ?"
                        " WHERE pipeline_id = ? AND run_id = ?",
                        (expected_partitions, _now(), pipeline_id, run_id),
                    )
                row = self._select_run(connection, pipeline_id, run_id)
            else:
                now = _now()
                connection.execute(
                    "INSERT INTO pipeline_runs"
                    " (pipeline_id, run_id, batch_date, expected_partitions,"
                    "  created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (pipeline_id, run_id, batch_date, expected_partitions, now, now),
                )
                row = self._select_run(connection, pipeline_id, run_id)
            assert row is not None
            run = self._run_row(row)
        return {"run": run, "created": existing is None}

    # ---- 回执入库 ----

    def ingest_receipt(
        self, pipeline_id: str, run_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """把一份实际回执按运行编号入库，并返回最新综合视图。

        ``payload``:
          receipt_type: quality_gate | partition_progress
          payload: 回执正文（实际行数、规则、分区等）
          batch_date: 可选，必须与运行日期一致
          expected_partitions: 可选，分区总数
          received_at: 可选，回执产生时间
        """
        pipeline_id = self._require_pipeline(pipeline_id)
        run_id = self._require_run_id(run_id)
        if not isinstance(payload, dict):
            raise ReceiptValidationError("回执请求体必须是对象")

        receipt_type = normalize_receipt_type(
            payload.get("receipt_type") or payload.get("type")
        )
        body = payload.get("payload")
        if body is None:
            body = payload.get("data")
        if not isinstance(body, dict):
            raise ReceiptValidationError("回执缺少 payload 对象")

        received_at = str(payload.get("received_at") or _now())

        with self.db.transaction() as connection:
            run_row = self._select_run(connection, pipeline_id, run_id)
            if run_row is None:
                raise RunNotFoundError(
                    f"运行编号 {run_id} 未在管线 {pipeline_id} 下登记，请先登记运行"
                )
            run = self._run_row(run_row)
            batch_date = run["batch_date"]

            provided_date = payload.get("batch_date")
            if provided_date and str(provided_date) != batch_date:
                raise ReceiptConflictError(
                    f"回执日期 {provided_date} 与运行日期 {batch_date} 不一致"
                )

            expected = self._as_expected(payload.get("expected_partitions"))
            if expected > run["expected_partitions"]:
                connection.execute(
                    "UPDATE pipeline_runs SET expected_partitions = ?, updated_at = ?"
                    " WHERE pipeline_id = ? AND run_id = ?",
                    (expected, _now(), pipeline_id, run_id),
                )
                run["expected_partitions"] = expected

            enriched = self._enrich_body(receipt_type, body, batch_date)
            # 先做完整校验，再写库：非法回执不会留下半条记录。
            if receipt_type == QUALITY_GATE:
                normalize_quality_gate(enriched)
            else:
                normalize_partition_progress(enriched, batch_date=batch_date)

            content_hash = _hash(enriched)
            latest = self._latest_receipt(
                connection, pipeline_id, run_id, receipt_type
            )
            if latest is not None and latest["content_hash"] == content_hash:
                revision = latest["revision"]
                stored_payload = json.loads(latest["payload_json"])
                deduplicated = True
            else:
                revision = (latest["revision"] if latest else 0) + 1
                connection.execute(
                    "INSERT INTO run_receipts"
                    " (pipeline_id, run_id, receipt_type, revision, content_hash,"
                    "  payload_json, batch_date, received_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        pipeline_id, run_id, receipt_type, revision, content_hash,
                        json.dumps(enriched, ensure_ascii=False, sort_keys=True),
                        batch_date, received_at,
                    ),
                )
                stored_payload = enriched
                deduplicated = False

            connection.execute(
                "UPDATE pipeline_runs SET updated_at = ?, expected_partitions = ?"
                " WHERE pipeline_id = ? AND run_id = ?",
                (_now(), max(run["expected_partitions"], expected), pipeline_id, run_id),
            )

        report = self.get_run_report(pipeline_id, run_id)
        return {
            "ok": True,
            "message": (
                f"{RECEIPT_LABELS[receipt_type]}已登记"
                + (f"（修订 {revision}）" if not deduplicated else "，内容未变化")
            ),
            "receipt": {
                "receipt_type": receipt_type,
                "label": RECEIPT_LABELS[receipt_type],
                "revision": revision,
                "content_hash": content_hash,
                "received_at": received_at,
                "deduplicated": deduplicated,
                "payload": stored_payload,
            },
            "report": report,
        }

    # ---- 综合报告 ----

    def get_run_report(self, pipeline_id: str, run_id: str) -> dict[str, Any]:
        """按运行编号返回当前综合报告；未登记运行直接 404 语义。"""
        pipeline_id = self._require_pipeline(pipeline_id)
        run_id = self._require_run_id(run_id)
        with self.db.reader() as connection:
            run_row = self._select_run(connection, pipeline_id, run_id)
            if run_row is None:
                raise RunNotFoundError(
                    f"运行编号 {run_id} 未在管线 {pipeline_id} 下登记"
                )
            run = self._run_row(run_row)
            receipts = self._all_latest_receipts(connection, pipeline_id, run_id)
            latest_confirmed, confirmed_scope = self._last_confirmed_for_review(
                connection, pipeline_id, run_id
            )
        return self._compose_report(run, receipts, latest_confirmed, confirmed_scope)

    def confirm_report(self, pipeline_id: str, run_id: str) -> dict[str, Any]:
        """把当前完整报告固化为可回看的确认版本。

        只有状态为 ``complete``（两份回执齐全、同轮、行数自洽）才允许发布；
        重复确认同样内容不会新增快照。
        """
        pipeline_id = self._require_pipeline(pipeline_id)
        run_id = self._require_run_id(run_id)

        with self.db.transaction() as connection:
            run_row = self._select_run(connection, pipeline_id, run_id)
            if run_row is None:
                raise RunNotFoundError(
                    f"运行编号 {run_id} 未在管线 {pipeline_id} 下登记"
                )
            run = self._run_row(run_row)
            receipts = self._all_latest_receipts(connection, pipeline_id, run_id)
            current = self._compose_report(run, receipts, None)
            report_data = current["current"]
            if report_data["state"] != "complete":
                raise ReceiptConflictError(
                    "当前报告不完整，不能发布："
                    + "；".join(report_data["issues"] or ["缺少回执"])
                )

            report_data["published"] = True
            # 只对报告语义内容做指纹：生成时间、运行 updated_at 等易变字段不参与，
            # 这样同一轮完整事实重复确认不会新增快照。
            canonical = {
                key: report_data.get(key)
                for key in (
                    "run_id", "pipeline_id", "batch_date", "state",
                    "quality", "progress", "alerts", "alert_items",
                    "evidence", "health", "issues", "receipt_summary", "published",
                )
            }
            report_hash = _hash(canonical)
            existing = connection.execute(
                "SELECT * FROM run_reports"
                " WHERE pipeline_id = ? AND run_id = ? AND report_hash = ?",
                (pipeline_id, run_id, report_hash),
            ).fetchone()
            if existing is None:
                confirmed_at = _now()
                report_data["generated_at"] = confirmed_at
                cursor = connection.execute(
                    "INSERT INTO run_reports"
                    " (pipeline_id, run_id, state, batch_date, report_hash,"
                    "  report_json, confirmed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        pipeline_id, run_id, report_data["state"], run["batch_date"],
                        report_hash, json.dumps(report_data, ensure_ascii=False),
                        confirmed_at,
                    ),
                )
                report_id = int(cursor.lastrowid)
                message = "综合报告已确认发布"
            else:
                report_id = int(existing["id"])
                confirmed_at = str(existing["confirmed_at"])
                report_data = json.loads(existing["report_json"])
                message = "综合报告此前已确认，未重复写入"

            connection.execute(
                "UPDATE pipeline_runs SET confirmed_at = ?, updated_at = ?"
                " WHERE pipeline_id = ? AND run_id = ?",
                (confirmed_at, _now(), pipeline_id, run_id),
            )

        return {
            "ok": True,
            "message": message,
            "report_id": report_id,
            "confirmed_at": confirmed_at,
            "report": report_data,
        }

    def get_confirmed_report(
        self, pipeline_id: str, run_id: str, report_id: int | None = None
    ) -> dict[str, Any]:
        """读取数据库中已确认的报告快照（默认最新一份）。"""
        pipeline_id = self._require_pipeline(pipeline_id)
        run_id = self._require_run_id(run_id)
        with self.db.reader() as connection:
            if self._select_run(connection, pipeline_id, run_id) is None:
                raise RunNotFoundError(
                    f"运行编号 {run_id} 未在管线 {pipeline_id} 下登记"
                )
            if report_id is None:
                row = self._latest_confirmed_row(connection, pipeline_id, run_id)
            else:
                row = connection.execute(
                    "SELECT * FROM run_reports"
                    " WHERE pipeline_id = ? AND run_id = ? AND id = ?",
                    (pipeline_id, run_id, report_id),
                ).fetchone()
            if row is None:
                raise ReportNotConfirmedError(
                    f"运行 {run_id} 没有已确认的综合报告"
                    + (f"（report_id={report_id}）" if report_id else "")
                )
            return {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "report_id": int(row["id"]),
                "confirmed_at": str(row["confirmed_at"]),
                "batch_date": str(row["batch_date"]),
                "report": json.loads(row["report_json"]),
            }

    def list_runs(
        self, pipeline_id: str | None = None, batch_date: str | None = None
    ) -> list[dict[str, Any]]:
        """列出已登记运行，附带当前回执齐全度，供值班复核。"""
        clauses: list[str] = []
        params: list[Any] = []
        if pipeline_id:
            clauses.append("pipeline_id = ?")
            params.append(pipeline_id)
        if batch_date:
            clauses.append("batch_date = ?")
            params.append(batch_date)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.db.reader() as connection:
            rows = connection.execute(
                "SELECT * FROM pipeline_runs" + where + " ORDER BY batch_date DESC, run_id DESC",
                params,
            ).fetchall()
            items = []
            for row in rows:
                receipts = self._all_latest_receipts(
                    connection, row["pipeline_id"], row["run_id"]
                )
                items.append({
                    **self._run_row(row),
                    "received_receipts": sorted(receipts),
                    "missing_receipts": [
                        t for t in RECEIPT_TYPES if t not in receipts
                    ],
                    "receipt_revisions": {
                        receipt_type: info["revision"]
                        for receipt_type, info in receipts.items()
                    },
                })
        return items

    def resolve_pipeline(self, run_id: str, pipeline_id: str | None = None) -> str:
        """核验运行编号归属；未登记、或跨管线不唯一时拒绝。"""
        run_id = self._require_run_id(run_id)
        if pipeline_id:
            pipeline = str(pipeline_id).strip()
            with self.db.reader() as connection:
                if self._select_run(connection, pipeline, run_id) is None:
                    raise RunNotFoundError(
                        f"运行编号 {run_id} 未在管线 {pipeline} 下登记"
                    )
            return pipeline

        matches = [item for item in self.list_runs() if item["run_id"] == run_id]
        if not matches:
            raise RunNotFoundError(f"运行编号 {run_id} 未登记")
        pipelines = {item["pipeline_id"] for item in matches}
        if len(pipelines) > 1:
            raise ReceiptValidationError(
                f"运行编号 {run_id} 属于多条管线，请同时提供 pipeline_id"
            )
        return matches[0]["pipeline_id"]

    # ---- 内部 ----

    @staticmethod
    def _require_pipeline(pipeline_id: str) -> str:
        pipeline = str(pipeline_id or "").strip()
        if not pipeline:
            raise ReceiptValidationError("缺少管线标识")
        return pipeline

    @staticmethod
    def _require_run_id(run_id: Any) -> str:
        value = str(run_id or "").strip()
        if not value:
            raise ReceiptValidationError("缺少运行编号 run_id")
        if len(value) > RUN_ID_PATTERN_MAX:
            raise ReceiptValidationError("运行编号过长")
        return value

    @staticmethod
    def _require_batch_date(value: Any) -> str:
        text = str(value or "").strip()
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError as exc:
            raise ReceiptValidationError("batch_date 必须是 YYYY-MM-DD 格式的日期") from exc
        return text

    @staticmethod
    def _as_expected(value: Any) -> int:
        if value is None or str(value).strip() == "":
            return 0
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise ReceiptValidationError("expected_partitions 必须是整数") from exc
        if number < 0:
            raise ReceiptValidationError("expected_partitions 不能为负数")
        return number

    @staticmethod
    def _run_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "pipeline_id": str(row["pipeline_id"]),
            "run_id": str(row["run_id"]),
            "batch_date": str(row["batch_date"]),
            "expected_partitions": int(row["expected_partitions"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "confirmed_at": str(row["confirmed_at"]) if row["confirmed_at"] else None,
        }

    @staticmethod
    def _select_run(
        connection: sqlite3.Connection, pipeline_id: str, run_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM pipeline_runs WHERE pipeline_id = ? AND run_id = ?",
            (pipeline_id, run_id),
        ).fetchone()

    @staticmethod
    def _latest_receipt(
        connection: sqlite3.Connection,
        pipeline_id: str,
        run_id: str,
        receipt_type: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM run_receipts"
            " WHERE pipeline_id = ? AND run_id = ? AND receipt_type = ?"
            " ORDER BY revision DESC LIMIT 1",
            (pipeline_id, run_id, receipt_type),
        ).fetchone()

    def _all_latest_receipts(
        self, connection: sqlite3.Connection, pipeline_id: str, run_id: str
    ) -> dict[str, dict[str, Any]]:
        receipts: dict[str, dict[str, Any]] = {}
        for receipt_type in RECEIPT_TYPES:
            row = self._latest_receipt(connection, pipeline_id, run_id, receipt_type)
            if row is None:
                continue
            receipts[receipt_type] = {
                "receipt_type": receipt_type,
                "label": RECEIPT_LABELS[receipt_type],
                "revision": int(row["revision"]),
                "content_hash": str(row["content_hash"]),
                "received_at": str(row["received_at"]),
                "batch_date": str(row["batch_date"]),
                "payload": json.loads(row["payload_json"]),
            }
        return receipts

    @staticmethod
    def _latest_confirmed_row(
        connection: sqlite3.Connection, pipeline_id: str, run_id: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM run_reports WHERE pipeline_id = ? AND run_id = ?"
            " ORDER BY id DESC LIMIT 1",
            (pipeline_id, run_id),
        ).fetchone()

    def _latest_confirmed(
        self, connection: sqlite3.Connection, pipeline_id: str, run_id: str
    ) -> dict[str, Any] | None:
        row = self._latest_confirmed_row(connection, pipeline_id, run_id)
        if row is None:
            return None
        return {
            "report_id": int(row["id"]),
            "confirmed_at": str(row["confirmed_at"]),
            "batch_date": str(row["batch_date"]),
            "report": json.loads(row["report_json"]),
        }

    def _last_confirmed_for_review(
        self, connection: sqlite3.Connection, pipeline_id: str, run_id: str
    ) -> tuple[dict[str, Any] | None, str]:
        """同一次运行已确认的快照优先；否则回退到同管线最近一次完整结果。

        回退结果会标明 ``scope=previous_run``，避免把上一次运行的证据
        误当成这一次的。
        """
        own = self._latest_confirmed(connection, pipeline_id, run_id)
        if own is not None:
            return own, "same_run"

        row = connection.execute(
            "SELECT * FROM run_reports WHERE pipeline_id = ?"
            " ORDER BY confirmed_at DESC, id DESC LIMIT 1",
            (pipeline_id,),
        ).fetchone()
        if row is None:
            return None, "none"
        return {
            "report_id": int(row["id"]),
            "run_id": str(row["run_id"]),
            "confirmed_at": str(row["confirmed_at"]),
            "batch_date": str(row["batch_date"]),
            "report": json.loads(row["report_json"]),
        }, "previous_run"

    def _compose_report(
        self,
        run: dict[str, Any],
        receipts: dict[str, dict[str, Any]],
        latest_confirmed: dict[str, Any] | None,
        confirmed_scope: str = "none",
    ) -> dict[str, Any]:
        quality: QualityGateFacts | None = None
        partitions: PartitionProgressFacts | None = None
        if QUALITY_GATE in receipts:
            quality = normalize_quality_gate(receipts[QUALITY_GATE]["payload"])
        if PARTITION_PROGRESS in receipts:
            partitions = normalize_partition_progress(
                receipts[PARTITION_PROGRESS]["payload"], batch_date=run["batch_date"]
            )

        receipt_meta = {
            receipt_type: {
                "revision": info["revision"],
                "received_at": info["received_at"],
                "content_hash": info["content_hash"],
                "label": info["label"],
            }
            for receipt_type, info in receipts.items()
        }
        current = compute_run_report(
            run=run, quality=quality, partitions=partitions, receipt_meta=receipt_meta
        )
        current["generated_at"] = _now()
        current["run"] = run

        previous_complete = latest_confirmed if confirmed_scope != "none" else None
        return {
            "run": run,
            "state": current["state"],
            "received_receipts": current["evidence"]["received_receipts"],
            "missing_receipts": current["evidence"]["missing_receipts"],
            "receipts": receipt_meta,
            "current": current,
            "last_confirmed": previous_complete,
            "last_confirmed_scope": confirmed_scope,
            "has_confirmed_report": confirmed_scope == "same_run",
        }

    # ---- 与质量执行记录交叉核对 ----

    def _enrich_body(
        self, receipt_type: str, body: dict[str, Any], batch_date: str
    ) -> dict[str, Any]:
        """把回执与库内质量执行记录对齐，拒绝跨日期或跨批次的证据。"""
        enriched = json.loads(json.dumps(body, ensure_ascii=False))  # 深拷贝

        if receipt_type == QUALITY_GATE:
            run_ref = enriched.get("validation_run_id")
            if run_ref is not None:
                record = self._validation_record(int(run_ref))
                self._check_validation_record(record, batch_date, enriched)
                self._fill_from_validation_record(enriched, record)
            return enriched

        for index, item in enumerate(enriched.get("partitions") or []):
            if not isinstance(item, dict) or item.get("validation_run_id") is None:
                continue
            record = self._validation_record(int(item["validation_run_id"]))
            partition_date = self._partition_date(item)
            if partition_date and str(record.get("校验分区", "")) != partition_date:
                raise ReceiptConflictError(
                    f"分区 {item.get('partition')} 引用的校验批次 "
                    f"{record.get('批次标识')} 属于分区 {record.get('校验分区')}，"
                    f"与 {partition_date} 不是同一轮"
                )
            if record.get("目标表") and enriched.get("target_table") and (
                str(record["目标表"]) != str(enriched["target_table"])
            ):
                raise ReceiptConflictError(
                    f"分区 {item.get('partition')} 引用的校验表 "
                    f"{record['目标表']} 与回执目标表 {enriched['target_table']} 不一致"
                )
        return enriched

    @staticmethod
    def _partition_date(item: dict[str, Any]) -> str:
        raw = item.get("batch_date") or item.get("partition_date")
        if raw:
            return str(raw)
        key = str(item.get("partition") or item.get("key") or "")
        return key[:10] if len(key) >= 10 else ""

    @staticmethod
    def _validation_record(entry_id: int) -> dict[str, Any]:
        record = store.find("validation_run", entry_id)
        if record is None:
            raise ReceiptValidationError(
                f"质量执行记录 {entry_id} 不存在，无法关联实际运行回执"
            )
        return record

    def _check_validation_record(
        self,
        record: dict[str, Any],
        batch_date: str,
        payload: dict[str, Any],
    ) -> None:
        record_date = str(record.get("校验分区", ""))
        if record_date and record_date != batch_date:
            raise ReceiptConflictError(
                f"质量执行记录 {record.get('批次标识')} 的校验分区 {record_date}"
                f" 与运行日期 {batch_date} 不一致，不能拼接两个日期的证据"
            )
        target = str(record.get("目标表", ""))
        payload_target = str(payload.get("target_table") or "")
        if target and payload_target and target != payload_target:
            raise ReceiptConflictError(
                f"质量执行记录目标表 {target} 与回执目标表 {payload_target} 不一致"
            )
        if payload.get("validation_batch") and (
            str(payload["validation_batch"]) != str(record.get("批次标识"))
        ):
            raise ReceiptConflictError(
                f"回执批次标识 {payload['validation_batch']} 与质量执行记录 "
                f"{record.get('批次标识')} 不一致"
            )

    @staticmethod
    def _fill_from_validation_record(
        payload: dict[str, Any], record: dict[str, Any]
    ) -> None:
        """用质量执行记录补齐或核对实际行数。"""
        payload.setdefault("target_table", record.get("目标表", ""))
        payload.setdefault("validation_batch", record.get("批次标识", ""))
        if not payload.get("validation_status"):
            payload["validation_status"] = record.get("status", "")

        record_counts: RowCounts
        try:
            record_counts = RowCounts(
                total_rows=int(str(record.get("总行数", "0"))),
                passed_rows=int(str(record.get("通过行数", "0"))),
                failed_rows=int(str(record.get("失败行数", "0"))),
            )
        except (ValueError, ReceiptValidationError) as exc:
            raise ReceiptConflictError(
                f"质量执行记录 {record.get('批次标识')} 的行数不完整，无法作为实际回执"
            ) from exc

        provided = [
            key for key in ("total_rows", "passed_rows", "failed_rows")
            if payload.get(key) is not None
        ]
        if not provided:
            payload.update(record_counts.to_dict())
            return

        try:
            payload_counts = RowCounts.from_mapping(payload)
        except ReceiptValidationError as exc:
            raise ReceiptConflictError(
                f"回执行数与质量执行记录 {record.get('批次标识')} 冲突：{exc}"
            ) from exc
        if (
            payload_counts.total_rows != record_counts.total_rows
            or payload_counts.failed_rows != record_counts.failed_rows
        ):
            raise ReceiptConflictError(
                f"回执行数（{payload_counts.total_rows}/{payload_counts.failed_rows}）"
                f"与质量执行记录 {record.get('批次标识')} "
                f"（{record_counts.total_rows}/{record_counts.failed_rows}）不一致"
            )


run_report_service = RunReportService()
