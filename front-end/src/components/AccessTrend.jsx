import React, { Component } from "react";
import ReactEcharts from "echarts-for-react";
const axios = require("axios");

export default class AccessTrend extends Component {
  state = {
    data: []
  };

  componentDidMount() {
    axios
      .get("http://localhost:3088/api/v1/access-trend")
      .then(res => this.setState({ data: res.data }))
      .catch(err => console.error(err));
  }

  getDisplayData = (originData, pattern) => {
    if (!originData || originData.length < 1) return;
    let label = [];
    let inValue = [];
    let outValue = [];

    originData = originData.reverse();

    originData.map(d => {
      label.push(d._id);
      if (pattern == "bytes") {
        inValue.push(d.inBytes);
        outValue.push(d.outBytes)
      } else {
        inValue.push(d.inPackets);
        outValue.push(d.outPackets);
      }
    });

    return {
      label,
      inValue,
      outValue
    };
  };
  getOption = displayData => ({

    backgroundColor: "#fff",
    legend: {
      data: ["inflow", "outflow"],
      align: "left",
      left: 10
    },

    tooltip: {},
    xAxis: {
      data: displayData["label"],
      name: "X Axis",
      silent: false,
      axisLine: { onZero: true },
      splitLine: { show: false },
      splitArea: { show: false }
    },
    yAxis: {
      inverse: false,
      splitArea: { show: false }
    },
    grid: {
      left: 100
    },

    series: [
      
        {
          name: "outflow",
          type: "bar",
          stack: "one",
          data: displayData["outValue"],
          itemStyle: {
            normal: {
              color: "#36cfc9"
            }
          }
        },
        {
            name: "inflow",
            type: "bar",
            stack: "one",
            data: displayData["inValue"],
            itemStyle: {
              normal: {
                color: "#1890ff"
              }
            }
          },
    ]
  });
  render() {
    const displayData = this.getDisplayData(this.state.data, "bytes");
    console.log(displayData);
    return (
      <div class="ui card" style={{ width: "100%", marginTop: 20, marginBottom: 20 }}>
        <div class="content">
          <div class="header">Access Trend</div>
          <div class="meta">Data from MongoDB. Connect via HTTP request.</div>
        </div>
        <div class="content">
          {displayData && (
            <ReactEcharts
              option={this.getOption(displayData)}
              notMerge={true}
              lazyUpdate={true}
              style={{ marginTop: 30, height: 350 }}
            />
          )}
        </div>
      </div>
    );
  }
}
