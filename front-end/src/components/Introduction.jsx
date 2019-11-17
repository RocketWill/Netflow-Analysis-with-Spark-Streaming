import React, { Component } from "react";
import flow from '../../src/flow.png';

export default class Introduction extends Component {
  render() {
    return (
      <div class="ui cards">
        <div class="ui teal fluid card" style={{ marginTop: 40 }}>
          <div class="content">
            <div class="header">实验简介</div>
            <ul class="list">
              <li class="content">使用 pmacct 监控流量，并写入 Kafka 中</li>
              <li class="content">
                使用 Spark Streaming 消费 Kafka 中的数据，写入
                MongoDB，或写入新的 Kafka topic
              </li>
              <li class="content">
                运用 Websocket 实时将 Kafka 中的数据传至前端，或使用 Http 将
                MongoDB 数据传至前端
              </li>
            </ul>
            <div class="ui divider"></div>
            <div class="header">流程</div>
            <img src={flow} style={{width: "100%"}} class="ui massive image" />
          </div>
        </div>

        
      </div>
    );
  }
}
