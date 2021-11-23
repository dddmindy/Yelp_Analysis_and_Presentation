<template>
  <div :class="className" :style="{height:height,width:width}" />
</template>

<script>
import echarts from 'echarts'
require('echarts/theme/macarons') // echarts theme
import resize from './mixins/resize'

const animationDuration = 6000

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
        backgroundColor: '#344b58',
        title: {
          text: 'Top 10 Cities with most businesses',
          textStyle: {
            fontWeight: 'normal',
            fontSize: 16,
            color: '#F1F1F3'
          }
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: { // 坐标轴指示器，坐标轴触发有效
            type: 'shadow' // 默认为直线，可选为：'line' | 'shadow'
          }
        },
        grid: {
          top: 50,
          left: '3%',
          right: '3%',
          bottom: '5%',
          containLabel: true
        },
        xAxis: [{
          type: 'category',
          data: ['Waltham', 'Brookline', 'Newton', 'Quincy', 'Natick', 'Boston', 'Medford', 'Cambridge', 'Salem', 'Somerville'],
          axisTick: {
            alignWithLabel: true
          }
        }],
        yAxis: [{
          type: 'value',
          axisTick: {
            show: false
          }
        }],
        series: [{
          name: 'number of businesses',
          type: 'bar',
          stack: 'vistors',
          barWidth: '60%',
          data: [662, 741, 634, 784, 522, 5923, 500, 1792, 511, 933],
          animationDuration
        }]
      })
    }
  }
}
</script>
