<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">ETL编排与数据质量中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向数据仓库ETL作业场景，提供流水线编排、质量规则、依赖调度和回刷补偿的ETL治理平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "综合运行报告", path: "/run_report" }, { label: "流水线定义", path: "/pipeline_def" }, { label: "任务节点", path: "/task_node" }, { label: "质量规则", path: "/quality_rule" }, { label: "质量执行", path: "/validation_run" }, { label: "依赖调度", path: "/depend_scheduler" }, { label: "滑动窗口", path: "/sliding_window" }, { label: "回刷补偿", path: "/rerun_compensation" }, { label: "水位追踪", path: "/watermark_tracker" }, { label: "资源池", path: "/resource_pool" }, { label: "血缘图谱", path: "/lineage_graph" }, { label: "异常捕获", path: "/exception_capture" }, { label: "模板仓库", path: "/template_store" }, { label: "分区扫描", path: "/partition_scan" }, { label: "配置迁移", path: "/config_migration" }, { label: "Schema演进", path: "/schema_evolution" }, { label: "回填追踪", path: "/backfill_tracker" }, { label: "成本分析", path: "/cost_analysis" }, { label: "告警策略", path: "/alert_policy" }]
</script>
