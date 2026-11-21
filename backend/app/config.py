"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# 回执与综合报告默认落在后端目录的 var/ 下；通过 RUN_REPORT_DB 可以换到
# 独立磁盘，容器里也可以挂一个持久卷，进程重启后已确认报告仍然可查。
DEFAULT_RUN_REPORT_DB = str(
    Path(__file__).resolve().parents[1] / "var" / "run_reports.sqlite3"
)


@dataclass(frozen=True)
class Settings:
    app_name: str = "ETL编排与数据质量中心"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    run_report_db: str = field(
        default_factory=lambda: os.environ.get("RUN_REPORT_DB", DEFAULT_RUN_REPORT_DB)
    )


settings = Settings()
