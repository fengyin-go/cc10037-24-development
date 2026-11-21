"""业务路由汇总：资源路由按模块分派，业务路由补充编排与运行报告。"""
from __future__ import annotations

from app.routers import business, resource, run_report

# 运行报告路由先于通用资源路由注册，避免 /api/runs 被 /api/{module} 抢占。
ROUTERS = [run_report, business, resource]
