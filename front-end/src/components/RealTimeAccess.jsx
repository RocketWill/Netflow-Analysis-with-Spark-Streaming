import React, { Component } from "react";
import echarts from "echarts/lib/echarts";
import ReactEcharts from "echarts-for-react";
import socketIOClient from "socket.io-client";

export default class RealTimeAccess extends Component {
  state = {
    response: 0,
    endpoint: "http://127.0.0.1:4001",
    label: [],
    value: []
  };

  componentDidMount() {
    const { endpoint } = this.state;
    const socket = socketIOClient(endpoint);
    socket.on("broad", data => {
      this.setState({label: [...this.state.label, data.time]});
      this.setState({value: [...this.state.value, data.bytes]});
    });
  }

  getOption = (label, value) => (
    {
        xAxis: {
            type: 'category',
            data: label
        },
        yAxis: {
            type: 'value'
        },
        series: [{
            data: value,
            type: 'line',
            smooth: true,
            itemStyle: {color: '#36cfc9'}
        }]
    }
    
  )
  render() {
      const {label, value} = this.state;
    return (
      <div
        class="ui card"
        style={{ width: "100%", marginTop: 20, marginBottom: 20 }}
      >
        <div class="content">
          <div class="header">Real Time Access Trend</div>
          <div class="meta">Data from Kafka. Connect via Websocket.</div>
        </div>
        <div class="content">
          <ReactEcharts
            option={this.getOption(label, value)}
            notMerge={true}
            lazyUpdate={true}
            style={{ marginTop: 30, height: 350 }}
          />
        </div>
      </div>
    );
  }
}
