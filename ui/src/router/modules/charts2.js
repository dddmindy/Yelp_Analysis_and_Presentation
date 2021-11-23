/** When your routing table is too long, you can split it into small modules**/

import Layout from '@/layout'

const charts2Router = {
  path: '/charts2',
  component: Layout,
  redirect: 'noRedirect',
  name: 'Charts2',
  meta: {
    title: 'Restaurant',
    icon: 'chart'
  },
  children: [{
    path: 'line',
    component: () => import('@/views/charts2/line'),
    name: 'LineChart2',
    meta: { title: 'Line Chart', noCache: true }
  },
  {
    path: 'keyboard',
    component: () => import('@/views/charts2/keyboard'),
    name: 'KeyboardChart2',
    meta: { title: 'Bar Chart', noCache: true }
  }]
}

export default charts2Router
