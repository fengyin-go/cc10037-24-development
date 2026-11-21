import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import RunReportView from '@/views/RunReportView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径必须先于 /:moduleKey，否则会被工作台抢走。
    { path: '/run_report', name: 'run_report', component: RunReportView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
