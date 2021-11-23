<template>
  <div :class="className" :style="{height:height,width:width}" />
</template>

<script>
import echarts from 'echarts'
require('echarts/theme/macarons') // echarts theme
import resize from './mixins/resize'

export default {
  mixins: [resize],
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '100%'
    },
    height: {
      type: String,
      default: '300px'
    }
  },
  data() {
    return {
      chart: null
    }
  },
  mounted() {
    this.$nextTick(() => {
      this.initChart()
    })
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    initChart() {
      this.chart = echarts.init(this.$el, 'macarons')

      this.chart.setOption({
        title: {
          text: 'Business situation-cities'
        },
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b} : {c}'
        },
        legend: {
          left: 'center',
          bottom: '10',
          data: ['Beaverton ', 'Portland', 'Vancouver ', 'Austin', 'Boulder', 'Boston', 'Atlanta', 'Cambridge', 'Columbus', 'Orlando', 'Columbus'] },
        series: [
          {
            name: 'number of businesses',
            type: 'pie',
            roseType: 'radius',
            radius: [15, 70],
            center: ['50%', '38%'],
            data: [
              { value: 1892, name: 'Beaverton' },
              { value: 15012, name: 'Portland' },
              { value: 11781, name: 'Vancouver' },
              { value: 17643, name: 'Austin' },
              { value: 2116, name: 'Boulder' },

              { value: 6882, name: 'Boston' },
              { value: 10709, name: 'Atlanta' },
              { value: 5706, name: 'Cambridge' },
              { value: 9038, name: 'Orlando' },
              { value: 5706, name: 'Columbus' }
            ],
            animationEasing: 'cubicInOut',
            animationDuration: 2600
          }
        ]
      })
    }
  }
}
</script>
