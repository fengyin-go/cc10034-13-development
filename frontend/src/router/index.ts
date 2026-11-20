import { createRouter, createWebHistory } from 'vue-router'

import DispatchBoardView from '@/views/DispatchBoardView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/dispatch_board', name: 'dispatch_board', component: DispatchBoardView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
