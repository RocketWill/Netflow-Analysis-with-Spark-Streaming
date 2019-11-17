<a href="#zhcn">简体中文</a> | <a href="#zhtw">繁體中文</a><br>
***本次实验使用的数据皆来自作者自身设备***  
<a id="zhcn"/>
## 实验说明：
运用 Apache Spark Streaming (Structured Streaming) 分析网络流量，并输出至前端 Dashboard 展示  
  
实验从两个面向对数据进行分析和处理：
#### 近实时分析
1. 运用 pmacct 监控网络流量，将数据保存至 Kafka 中。
2. 使用 ==Saprk Streaming== 消费 Kafka 的数据，最后根据需求  
    - 热数据：写入新的 Kafka Topic 中
    - 冷数据：写入 MongoDB 或 Elasticsearch 中
3. 前端展示
    - Kafka 数据通过 Websocket 与前台通信
    - MongoDB, Elasticsearch 通过 Http 请求与前台通信
#### 非实时分析
1. 使用 tcpdump 将截取到的报文输出成 pcap 文件并保存到特定文件夹下
2. 使用 ==Structured Streaming== 监控目录中的所有文件，将数据持久化至 MongoDB, Elasticsearch 中
3. 前端展示：MongoDB, Elasticsearch 通过 Http 请求与前台通信
## 分析流程图
![flow chart](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/process_flow.png)
## 成果展示
##### Real Time Access Trend  
![realtime access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/realtime-access-trend.gif)
##### Date Access Trend  
![date access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/date-access-trend.png)  
## Path1 近实时分析
#### 使用的工具和技术
1. pmacct
    - 用途：作为网络流量监视工具。使用 libpcap 进行采集后将数据导出至 Kafka。
    - 优点：
        - 开源免费软件包，多种被动网络监视功能工具集，可以实现分类、聚合、复制、导出网络流量数据
        - 支持  Linux, BSDs, Solaris 等
        - 可以采集 libpcap, Netlink/NFLOG, NetFlow v1/v5/v7/v8/v9, sFlow v2/v4/v5 和 IPFIX。
        - 可以导出到关系型数据库、NoSQL 数据库、RabbitMQ、Kafka、内存表、文件。
2. Apache Kafka
    - 用途：作为消息系统。保存由 pmacct 采集到的流量信息，并作为 Saprk Streaming 的数据源。
    - 优点：
        - 可靠性：分区机制、副本机制和容错机制
        - 可扩展性：支持集群规模的热扩展
        - 高性能：保证数据的高吞吐量
3. Apache Spark Streaming
    - 用途：接收实时流的数据，并根据一定的时间间隔拆分成一批批的数据，然后通过 Spark Engine 处理这些批数据。根据需求将结果输出至 Kafka 中或持久化到数据库中。
    - 编程模型：DStream 作为 Spark Streaming 的基础抽象，它代表持续性的数据流。这些数据流既可以通过外部输入源赖获取，也可以通过现有的 Dstream 的  transformation 操作来获得。在内部实现上，DStream 由一组时间序列上连续的 RDD 来表示。每个 RDD 都包含了自己特定时间间隔内的数据流。
4. Websocket
    - 用途：客户端与服务器端的持久连接。
    - 优点：由于部分数据需要实时传递给前端，而 HTTP 是非状态性的，每次都要重新传输；Websocket 只需要一次 HTTP 握手，整个通讯过程是建立在一次连接/状态中，避免了 HTTP 的非状态性，服务端会一直知道客户端信息，直到关闭请求。
#### 系统实现
##### 数据采集
1. 编写 pmacctd configuration 文件（pmacct 环境搭建在附录给出）  
    - example  

```
# pmacctd.conf

# 聚合规则
aggregate: src_mac, dst_mac, vlan, cos, etype, src_as, dst_as, peer_src_ip, peer_dst_ip, in_iface, out_iface, src_host, src_net, dst_host, dst_net, src_mask, dst_mask, src_port, dst_port, tcpflags, proto, tos, sampling_rate, timestamp_start, timestamp_end, timestamp_arrival
# 过滤（用法规则与 tcpdump 相同）
# pcap_filter: src net 10.0.0.0/16
plugins: kafka #插件名
kafka_output: json #输出格式
kafka_topic: netflow # Kafka Topic名 
kafka_refresh_time: 10 # 聚合10秒钟流量数据
# Kafka broker 信息
kafka_broker_host: 127.0.0.1
kafka_broker_port: 9092
```

2. 运行 
`
sudo pmacctd -f /path/to/config/file
`  
(-f 表示读取来自文件 的configuration)
3. 运行画面  
![pmacctd collect](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/pmacctd-collect.gif)
4. 查看 Kafka Consumer  
![kafka consumer](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/kafka-consumer.gif)
##### 数据处理 & 持久化
1. Spark Streaming 接入 Kafka  
- 初始化  
    
```
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo

# 创建 Spark 上下文
conf = SparkConf().setAppName("kafkaSparkStreaming").setMaster("spark://localhost:7077")
sc = SparkContext().getOrCreate(conf=conf)
ssc = StreamingContext(sc, 2)

# ...省略配置代码

# 使用高阶api接受数据（Receiver-based Approach）
# kafkaStream = KafkaUtils.createStream(streamingContext, [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
kafkaStream = KafkaUtils.createStream(ssc, zookeeper, "group-a", {topic: 1})
```

- 原数据格式

    
```
(None, '{"event_type": "purge", "iface_in": 0, "iface_out": 0, "ip_src": "192.168.178.80", "ip_dst": "192.168.178.1", "port_src": 22, "port_dst": 58608, "tcp_flags": "24", "ip_proto": "tcp", "tos": 18, "timestamp_start": "2019-11-11 06:18:00.043547", "timestamp_end": "1969-12-31 16:00:00.000000", "timestamp_arrival": "2019-11-11 06:18:00.043547", "packets": 1, "bytes": 168, "writer_id": "default_kafka/9190"}')
```
- 获取目标数据

```
targets = kafkaStream.map(lambda data: data[1])
```
- 整理成需要的格式  
截取原 ip、目标 ip、事件类型、报文数、字节数、协议、时间戳、原端口以及目标端口
```
def mapper(record):
    record = json.loads(record)
    res = {}
    res["ip_src"] = record.get("ip_src")
    res['ip_dst'] = record.get('ip_dst')
    res['event_type'] = record.get('event_type')
    res['packets'] = record.get('packets')
    res['bytes'] = record.get('bytes')
    res['protocol'] = record.get('ip_proto')
    res['timestamp'] = record.get('timestamp_start')
    res['port_src'] = record.get('port_src')
    res['port_dst'] = record.get('port_dst')
    return res
```

2. 持久化至 MongoDB

- 通常，创建一个连接对象有资源和时间的开支。因此为每个记录创建和销毁连接对象会导致非常高的开支，明显的减少系统的整体吞吐量。一个更好的解决办法是利用rdd.foreachPartition方法。 为 RDD 的 partition 创建一个连接对象，用这个连接对象发送 partition 中的所有记录。
```
def sendMongoDB(partition):
    # 初始化 mongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['kafka-netflow']
    collection_name = 'netflow'
    collection = db[collection_name]
    
    for record in partition:
        collection.insert_one(record)
        
    client.close()
```
- Transformation 操作

```
# 整理数据格式
process = targets.map(mapper)

# 持久化至 MongoDB
process.foreachRDD(lambda rdd: rdd.foreachPartition(sendMongoDB))
```
- MongoDB 中的数据  
<img src="https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/mongoDB-data.png" width="350px" /> <br> 
3. 输出至新的 Kafka Topic
- 初始化

```
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    linger_ms=1000,
    batch_size=1000,
)
def sendKafka(message):
    records = message.collect()
    for record in records:
        producer.send('newTopic', str(record).encode())
        producer.flush()
```
- Transformation 操作

```
# 整理数据格式
process = targets.map(mapper)

# 输出至 Kafka
process.foreachRDD(sendKafka)
```
- 问题：此方法似乎只能在 local 上运行，在集群上会报错。待解决。

## Path2 非实时分析
#### 使用的工具和技术
1. tcpdump
    - 用途：网络抓包，并将结果输出成 pcap 文件。
    - 介绍：可以将网络中传送的数据包的报文头完全截获下来提供分析，支持针对网络层、协议、主机、网络或端口的过滤，并提供and, or, not等逻辑语句来帮助用户去掉无用的信息。
2. Apache Structured Streaming
    - 用途：监控存放 pcap 文件的目录，并流式处理其中数据，最后将处理后的结果持久化至 MongoDB 或 Elasticsearch 中。
    - 优点：
        - Incremental query model：Structured Streaming 将会在新增的流式数据上不断执行增量查询，同时代码的写法和批处理 API （基于 Dataframe 和 Dataset API）完全一样。 
        - 复用 Spark SQL 执行引擎：Spark SQL 执行引擎做了非常多的优化工作，例如执行计划优化、codegen、内存管理等。使得 Structured Streaming 有高性能和高吞吐的能力。不同于 Spark Streaming 是基于 RDD 操作。
#### 系统实现
##### 数据采集
tcpdump 收集网络流量，example：

```
tcpdump -s 0 port -i eth0 -w mycap.pcap
```
其中，

```
-s 0：会将捕获字节设置为最大，即 65535，此捕获文件将不会被截断。
-i eth0：用于提供要捕获的以太网接口。 如果不使用此选项，则默认值为 eth0。
-w mypcap.pcap 将创建该 pcap 文件，该文件可以使用 wireshark 打开。
```
##### （问题）
由于 pcap 是二进制文件，目前还没找到比较理想的方式让 Spark 来读取，所以现在先用 tshark 将 pcap 文件转换成 json 格式，再作为 Structured Streaming 的数据源。
##### 数据处理 & 持久化
todo

## 前端展示
#### 使用的工具和技术
1. 前端框架：React.js
2. 后端框架：Express.js
3. 可视化库：ECharts
4. UI 库：Semantic UI
5. Websocket：
    - 用途：让 server 将最新的数据以最快的速度发送给 client
    - 介绍：WebSocket 是一种协议，是一种与 HTTP 同等的网络协议，两者都是应用层协议，基于 TCP。但是 WebSocket 是一种双向通信协议，在建立连接之后，WebSocket 的 server 与 client 都能主动向对方发送或接收数据。同时 WebSocket 在建立连接时需要借助 HTTP 协议，连接建立好了之后 client 与 server 之间的双向通信就与 HTTP 无关了。
    - Socket.IO：是一个封装了 Websocket、基于 Node 的 JavaScript 框架，包含 client 的 JavaScript 和 server 的 Node。当 Socket.IO 检测到当前环境不支持 WebSocket 时，能够自动地选择最佳的方式来实现网络的实时通信。
#### 系统实现
本次前端绘制两张图表，创建了两个服务器，一个用于处理 HTTP 请求；一个使用 Websocket 与前端通信：
- Date Access Trend：  
数据源是 MongoDB，依据日期统计当天的 inflow 和 outflow 数量（bytes），采用 HTTP 发送请求至后端获取数据，以叠加柱状图呈现。
- Real Time Access Trend：  
据源是 Kafka，前端通过 Websocket 实时获取后端消费 Kafka 的数据，每十秒更新一次，以动态折线图呈现。
##### Date Access Trend  
![date access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/date-access-trend.png)  
1. 原始数据 example：
```
_id: 5dc7f01be2e1d339bf8bf686
ip_src: "192.168.178.1"
ip_dst: "192.168.178.80"
event_type: "purge"
packets: 1
bytes: 52
protocol: "tcp"
timestamp: "2019-11-07 06:08:21.807555"
port_src: 50403
port_dst: 22
```
2. 聚合
- 欲使用之数据格式：  

```
{
    '_id': '日期', 
    'inBytes': '流入bytes数',
    'inPackets': '流入packets数',
    'outBytes': '流出bytes数',
    'outPackets': '流出packets数'
}
```

- Mongo Aggregation pipeline：  
其中 
`
192.168.178.80
`
为机器 IP


```
[
        {
          '$match': {
            'timestamp': {
              '$gte': '2019-11-05', 
              '$lte': '2019-11-15'
            }
          }
        }, {
          '$project': {
            'day': {
              '$substr': [
                '$timestamp', 0, 10
              ]
            }, 
            'inBytes': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_dst'
                  ]
                }, 
                'then': '$bytes', 
                'else': 0
              }
            }, 
            'outBytes': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_src'
                  ]
                }, 
                'then': '$bytes', 
                'else': 0
              }
            }, 
            'inPackets': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_dst'
                  ]
                }, 
                'then': '$packets', 
                'else': 0
              }
            }, 
            'outPackets': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_src'
                  ]
                }, 
                'then': '$packets', 
                'else': 0
              }
            }
          }
        }, {
          '$group': {
            '_id': '$day', 
            'inBytes': {
              '$sum': '$inBytes'
            }, 
            'inPackets': {
              '$sum': '$inPackets'
            }, 
            'outPackets': {
              '$sum': '$outPackets'
            }, 
            'outBytes': {
              '$sum': '$outBytes'
            }
          }
        }
      ]
```

##### Real Time Access Trend  
![realtime access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/realtime-access-trend.gif)
1. 创建 HTTP Server：  

```
const express = require('express')
const http = require('http')
const app = express()
const server = http.createServer(app)
const port = 3001 //端口号
server.listen(port, () => console.log(`Listening on port ${port}`))
```
2. Kafka 单个消费者，使用到
`
kafka-node
`
package（适用于 Apache Kafka 0.9 或之后的版本）  

```
const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({
    // broker
    kafkaHost: '192.168.178.80:9092'
});
const consumer = new Consumer(
    client,
    [{
        topic: "realTimeChart",
        fromOffset: 'latest' // 消费partition中最新的数据
    }], {
        autoCommit: true // 自动提交偏移量
    }
)
```
3. Websocket Server（使用 Socket.IO）  

```
const socketIO = require('socket.io')
const io = socketIO(server)
io.on('connection', socket => {
    console.log('User connected')

    socket.on('disconnect', () => {
        console.log('user disconnected')
    })

    consumer.on('message', (message) => {
        const netFlowMag = JSON.parse(message.value);
        const res = {
            time: netFlowMag.timestamp_arrival.substring(11,19),
            bytes: netFlowMag.bytes
        }
        socket.emit('broad', res);
    })
})
```
其中，消费出的数据范例：
```
{ topic: 'realTimeChart',
  value:
   '{"event_type": "purge", "iface_in": 0, "iface_out": 0, "ip_src": "192.168.178.1", "ip_dst": "192.168.178.80", "port_src": 62894, "port_dst": 9092, "tcp_flags": "16", "ip_proto": "tcp", "tos": 0, "timestamp_start": "2019-11-17 06:08:32.453958", "timestamp_end": "1969-12-31 16:00:00.000000", "timestamp_arrival": "2019-11-17 06:08:32.453958", "packets": 1, "bytes": 52, "writer_id": "default_kafka/56419"}',
  offset: 542178,
  partition: 0,
  highWaterOffset: 542179,
  key: null }
```
需要的是其中的 value 属性，并返回 value 中的 bytes。

4. Websocket Client（使用 Socket.IO Client）  
React 关键代码：
```
state = {
    response: 0,
    endpoint: "http://127.0.0.1:3001", //server地址
    label: [],
    value: []
};

componentDidMount() {
    const { endpoint } = this.state;
    const socket = socketIOClient(endpoint);
    socket.on("broad", data => {
      // 将数据append并更新至state 
      this.setState({label: [...this.state.label, data.time]});
      this.setState({value: [...this.state.value, data.bytes]});
    });
}
```

## 附录
#### pmacct 安装
1. 安装 libpcap-dev
`
$ sudo apt-get install libpcap-dev 
`
2. 安装 librdkafka 

```
$ git clone https://github.com/edenhill/librdkafka.git
$ cd librdkafka
$ ./configure
$ make
$ make install
```
3. 安装 libjansson-dev `$ apt-get install libpcap-dev libjansson-dev`
4. 安装带有选项的 pmacct 以使用 kafka  
    - 安装依赖  
    `$ apt-get install pkg-config libtool autoconf automake bash`
    - Build GitHub code:
```
$ git clone https://github.com/pmacct/pmacct.git
$ cd pmacct
$ ./autogen.sh
$ ./configure --enable-kafka --enable-jansson --enable-ipv6
$ make
$ make install [with super-user permission]
```
5. 确认 `$ pmacctd -V`， output：

```
Promiscuous Mode Accounting Daemon, pmacctd 1.7.4-git (20191031-00)

Arguments:
 '--enable-kafka' '--enable-jansson' '--enable-ipv6' '--enable-l2' '--enable-64bit' '--enable-traffic-bins' '--enable-bgp-bins' '--enable-bmp-bins' '--enable-st-bins'

Libs:
libpcap version 1.8.1
rdkafka 1.2.2-RC1-1-g8d44dd-dirty
jansson 2.11

System:
Linux 4.15.0-20-generic #21-Ubuntu SMP Tue Apr 24 06:16:15 UTC 2018 x86_64

Compiler:
gcc 7.4.0

For suggestions, critics, bugs, contact me: Paolo Lucente <paolo@pmacct.net>.
```

#### 参考资料
- https://github.com/linsomniac/pmacct
- https://spark.apache.org/streaming/
- https://socket.io/
- https://docs.mongodb.com/
- https://www.echartsjs.com/
- https://github.com/wurstmeister/kafka-docker/issues/534]

======

<a id="zhtw"/>  
## 繁體中文
## 实验说明：
运用 Apache Spark Streaming (Structured Streaming) 分析网络流量，并输出至前端 Dashboard 展示  
  
实验从两个面向对数据进行分析和处理：  
#### 近實時分析
1. 運用 pmacct 監控網絡流量，將數據保存至 Kafka 中。
2. 使用 ==Saprk Streaming== 消費 Kafka 的數據，最後根據需求  
    - 熱數據：寫入新的 Kafka Topic 中
    - 冷數據：寫入 MongoDB 或 Elasticsearch 中
3. 前端展示
    - Kafka 數據通過 Websocket 與前台通信
    - MongoDB, Elasticsearch 通過 Http 請求與前台通信
#### 非實時分析
1. 使用 tcpdump 將截取到的報文輸出成 pcap 文件並保存到特定文件夾下
2. 使用 ==Structured Streaming== 監控目錄中的所有文件，將數據持久化至 MongoDB, Elasticsearch 中
3. 前端展示：MongoDB, Elasticsearch 通過 Http 請求與前台通信
## 分析流程圖
![flow chart](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/process_flow.png)
## 成果展示
##### Real Time Access Trend  
![realtime access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/realtime-access-trend.gif)
##### Date Access Trend  
![date access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/date-access-trend.png)  
## Path1 近實時分析
#### 使用的工具和技術
1. pmacct
    - 用途：作為網絡流量監視工具。使用 libpcap 進行采集後將數據導出至 Kafka。
    - 優點：
        - 開源免費軟件包，多種被動網絡監視功能工具集，可以實現分類、聚合、覆制、導出網絡流量數據
        - 支持  Linux, BSDs, Solaris 等
        - 可以采集 libpcap, Netlink/NFLOG, NetFlow v1/v5/v7/v8/v9, sFlow v2/v4/v5 和 IPFIX。
        - 可以導出到關系型數據庫、NoSQL 數據庫、RabbitMQ、Kafka、內存表、文件。
2. Apache Kafka
    - 用途：作為消息系統。保存由 pmacct 采集到的流量信息，並作為 Saprk Streaming 的數據源。
    - 優點：
        - 可靠性：分區機制、副本機制和容錯機制
        - 可擴展性：支持集群規模的熱擴展
        - 高性能：保證數據的高吞吐量
3. Apache Spark Streaming
    - 用途：接收實時流的數據，並根據一定的時間間隔拆分成一批批的數據，然後通過 Spark Engine 處理這些批數據。根據需求將結果輸出至 Kafka 中或持久化到數據庫中。
    - 編程模型：DStream 作為 Spark Streaming 的基礎抽象，它代表持續性的數據流。這些數據流既可以通過外部輸入源賴獲取，也可以通過現有的 Dstream 的  transformation 操作來獲得。在內部實現上，DStream 由一組時間序列上連續的 RDD 來表示。每個 RDD 都包含了自己特定時間間隔內的數據流。
4. Websocket
    - 用途：客戶端與服務器端的持久連接。
    - 優點：由於部分數據需要實時傳遞給前端，而 HTTP 是非狀態性的，每次都要重新傳輸；Websocket 只需要一次 HTTP 握手，整個通訊過程是建立在一次連接/狀態中，避免了 HTTP 的非狀態性，服務端會一直知道客戶端信息，直到關閉請求。
#### 系統實現
##### 數據采集
1. 編寫 pmacctd configuration 文件（pmacct 環境搭建在附錄給出）  
    - example  

```
# pmacctd.conf

# 聚合規則
aggregate: src_mac, dst_mac, vlan, cos, etype, src_as, dst_as, peer_src_ip, peer_dst_ip, in_iface, out_iface, src_host, src_net, dst_host, dst_net, src_mask, dst_mask, src_port, dst_port, tcpflags, proto, tos, sampling_rate, timestamp_start, timestamp_end, timestamp_arrival
# 過濾（用法規則與 tcpdump 相同）
# pcap_filter: src net 10.0.0.0/16
plugins: kafka #插件名
kafka_output: json #輸出格式
kafka_topic: netflow # Kafka Topic名 
kafka_refresh_time: 10 # 聚合10秒鐘流量數據
# Kafka broker 信息
kafka_broker_host: 127.0.0.1
kafka_broker_port: 9092
```

2. 運行 
`
sudo pmacctd -f /path/to/config/file
`  
(-f 表示讀取來自文件 的configuration)
3. 運行畫面  
![pmacctd collect](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/pmacctd-collect.gif)
4. 查看 Kafka Consumer  
![kafka consumer](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/kafka-consumer.gif)
##### 數據處理 & 持久化
1. Spark Streaming 接入 Kafka  
- 初始化  
    
```
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo

# 創建 Spark 上下文
conf = SparkConf().setAppName("kafkaSparkStreaming").setMaster("spark://localhost:7077")
sc = SparkContext().getOrCreate(conf=conf)
ssc = StreamingContext(sc, 2)

# ...省略配置代碼

# 使用高階api接受數據（Receiver-based Approach）
# kafkaStream = KafkaUtils.createStream(streamingContext, [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
kafkaStream = KafkaUtils.createStream(ssc, zookeeper, "group-a", {topic: 1})
```

- 原數據格式

    
```
(None, '{"event_type": "purge", "iface_in": 0, "iface_out": 0, "ip_src": "192.168.178.80", "ip_dst": "192.168.178.1", "port_src": 22, "port_dst": 58608, "tcp_flags": "24", "ip_proto": "tcp", "tos": 18, "timestamp_start": "2019-11-11 06:18:00.043547", "timestamp_end": "1969-12-31 16:00:00.000000", "timestamp_arrival": "2019-11-11 06:18:00.043547", "packets": 1, "bytes": 168, "writer_id": "default_kafka/9190"}')
```
- 獲取目標數據

```
targets = kafkaStream.map(lambda data: data[1])
```
- 整理成需要的格式  
截取原 ip、目標 ip、事件類型、報文數、字節數、協議、時間戳、原端口以及目標端口
```
def mapper(record):
    record = json.loads(record)
    res = {}
    res["ip_src"] = record.get("ip_src")
    res['ip_dst'] = record.get('ip_dst')
    res['event_type'] = record.get('event_type')
    res['packets'] = record.get('packets')
    res['bytes'] = record.get('bytes')
    res['protocol'] = record.get('ip_proto')
    res['timestamp'] = record.get('timestamp_start')
    res['port_src'] = record.get('port_src')
    res['port_dst'] = record.get('port_dst')
    return res
```

2. 持久化至 MongoDB

- 通常，創建一個連接對象有資源和時間的開支。因此為每個記錄創建和銷毀連接對象會導致非常高的開支，明顯的減少系統的整體吞吐量。一個更好的解決辦法是利用rdd.foreachPartition方法。 為 RDD 的 partition 創建一個連接對象，用這個連接對象發送 partition 中的所有記錄。
```
def sendMongoDB(partition):
    # 初始化 mongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['kafka-netflow']
    collection_name = 'netflow'
    collection = db[collection_name]
    
    for record in partition:
        collection.insert_one(record)
        
    client.close()
```
- Transformation 操作

```
# 整理數據格式
process = targets.map(mapper)

# 持久化至 MongoDB
process.foreachRDD(lambda rdd: rdd.foreachPartition(sendMongoDB))
```
- MongoDB 中的數據  
<img src="https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/mongoDB-data.png" width="350px" /> <br> 
3. 輸出至新的 Kafka Topic
- 初始化

```
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    linger_ms=1000,
    batch_size=1000,
)
def sendKafka(message):
    records = message.collect()
    for record in records:
        producer.send('newTopic', str(record).encode())
        producer.flush()
```
- Transformation 操作

```
# 整理數據格式
process = targets.map(mapper)

# 輸出至 Kafka
process.foreachRDD(sendKafka)
```
- 問題：此方法似乎只能在 local 上運行，在集群上會報錯。待解決。

## Path2 非實時分析
#### 使用的工具和技術
1. tcpdump
    - 用途：網絡抓包，並將結果輸出成 pcap 文件。
    - 介紹：可以將網絡中傳送的數據包的報文頭完全截獲下來提供分析，支持針對網絡層、協議、主機、網絡或端口的過濾，並提供and, or, not等邏輯語句來幫助用戶去掉無用的信息。
2. Apache Structured Streaming
    - 用途：監控存放 pcap 文件的目錄，並流式處理其中數據，最後將處理後的結果持久化至 MongoDB 或 Elasticsearch 中。
    - 優點：
        - Incremental query model：Structured Streaming 將會在新增的流式數據上不斷執行增量查詢，同時代碼的寫法和批處理 API （基於 Dataframe 和 Dataset API）完全一樣。 
        - 覆用 Spark SQL 執行引擎：Spark SQL 執行引擎做了非常多的優化工作，例如執行計劃優化、codegen、內存管理等。使得 Structured Streaming 有高性能和高吞吐的能力。不同於 Spark Streaming 是基於 RDD 操作。
#### 系統實現
##### 數據采集
tcpdump 收集網絡流量，example：

```
tcpdump -s 0 port -i eth0 -w mycap.pcap
```
其中，

```
-s 0：會將捕獲字節設置為最大，即 65535，此捕獲文件將不會被截斷。
-i eth0：用於提供要捕獲的以太網接口。 如果不使用此選項，則默認值為 eth0。
-w mypcap.pcap 將創建該 pcap 文件，該文件可以使用 wireshark 打開。
```
##### （問題）
由於 pcap 是二進制文件，目前還沒找到比較理想的方式讓 Spark 來讀取，所以現在先用 tshark 將 pcap 文件轉換成 json 格式，再作為 Structured Streaming 的數據源。
##### 數據處理 & 持久化
todo

## 前端展示
#### 使用的工具和技術
1. 前端框架：React.js
2. 後端框架：Express.js
3. 可視化庫：ECharts
4. UI 庫：Semantic UI
5. Websocket：
    - 用途：讓 server 將最新的數據以最快的速度發送給 client
    - 介紹：WebSocket 是一種協議，是一種與 HTTP 同等的網絡協議，兩者都是應用層協議，基於 TCP。但是 WebSocket 是一種雙向通信協議，在建立連接之後，WebSocket 的 server 與 client 都能主動向對方發送或接收數據。同時 WebSocket 在建立連接時需要借助 HTTP 協議，連接建立好了之後 client 與 server 之間的雙向通信就與 HTTP 無關了。
    - Socket.IO：是一個封裝了 Websocket、基於 Node 的 JavaScript 框架，包含 client 的 JavaScript 和 server 的 Node。當 Socket.IO 檢測到當前環境不支持 WebSocket 時，能夠自動地選擇最佳的方式來實現網絡的實時通信。
#### 系統實現
本次前端繪制兩張圖表，創建了兩個服務器，一個用於處理 HTTP 請求；一個使用 Websocket 與前端通信：
- Date Access Trend：  
數據源是 MongoDB，依據日期統計當天的 inflow 和 outflow 數量（bytes），采用 HTTP 發送請求至後端獲取數據，以疊加柱狀圖呈現。
- Real Time Access Trend：  
據源是 Kafka，前端通過 Websocket 實時獲取後端消費 Kafka 的數據，每十秒更新一次，以動態折線圖呈現。
##### Date Access Trend  
![date access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/date-access-trend.png)  
1. 原始數據 example：
```
_id: 5dc7f01be2e1d339bf8bf686
ip_src: "192.168.178.1"
ip_dst: "192.168.178.80"
event_type: "purge"
packets: 1
bytes: 52
protocol: "tcp"
timestamp: "2019-11-07 06:08:21.807555"
port_src: 50403
port_dst: 22
```
2. 聚合
- 欲使用之數據格式：  

```
{
    '_id': '日期', 
    'inBytes': '流入bytes數',
    'inPackets': '流入packets數',
    'outBytes': '流出bytes數',
    'outPackets': '流出packets數'
}
```

- Mongo Aggregation pipeline：  
其中 
`
192.168.178.80
`
為機器 IP


```
[
        {
          '$match': {
            'timestamp': {
              '$gte': '2019-11-05', 
              '$lte': '2019-11-15'
            }
          }
        }, {
          '$project': {
            'day': {
              '$substr': [
                '$timestamp', 0, 10
              ]
            }, 
            'inBytes': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_dst'
                  ]
                }, 
                'then': '$bytes', 
                'else': 0
              }
            }, 
            'outBytes': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_src'
                  ]
                }, 
                'then': '$bytes', 
                'else': 0
              }
            }, 
            'inPackets': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_dst'
                  ]
                }, 
                'then': '$packets', 
                'else': 0
              }
            }, 
            'outPackets': {
              '$cond': {
                'if': {
                  '$eq': [
                    '192.168.178.80', '$ip_src'
                  ]
                }, 
                'then': '$packets', 
                'else': 0
              }
            }
          }
        }, {
          '$group': {
            '_id': '$day', 
            'inBytes': {
              '$sum': '$inBytes'
            }, 
            'inPackets': {
              '$sum': '$inPackets'
            }, 
            'outPackets': {
              '$sum': '$outPackets'
            }, 
            'outBytes': {
              '$sum': '$outBytes'
            }
          }
        }
      ]
```

##### Real Time Access Trend  
![realtime access trend](https://raw.githubusercontent.com/RocketWill/Netflow-Analysis-with-Spark-Streaming/master/images/realtime-access-trend.gif)
1. 創建 HTTP Server：  

```
const express = require('express')
const http = require('http')
const app = express()
const server = http.createServer(app)
const port = 3001 //端口號
server.listen(port, () => console.log(`Listening on port ${port}`))
```
2. Kafka 單個消費者，使用到
`
kafka-node
`
package（適用於 Apache Kafka 0.9 或之後的版本）  

```
const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({
    // broker
    kafkaHost: '192.168.178.80:9092'
});
const consumer = new Consumer(
    client,
    [{
        topic: "realTimeChart",
        fromOffset: 'latest' // 消費partition中最新的數據
    }], {
        autoCommit: true // 自動提交偏移量
    }
)
```
3. Websocket Server（使用 Socket.IO）  

```
const socketIO = require('socket.io')
const io = socketIO(server)
io.on('connection', socket => {
    console.log('User connected')

    socket.on('disconnect', () => {
        console.log('user disconnected')
    })

    consumer.on('message', (message) => {
        const netFlowMag = JSON.parse(message.value);
        const res = {
            time: netFlowMag.timestamp_arrival.substring(11,19),
            bytes: netFlowMag.bytes
        }
        socket.emit('broad', res);
    })
})
```
其中，消費出的數據範例：
```
{ topic: 'realTimeChart',
  value:
   '{"event_type": "purge", "iface_in": 0, "iface_out": 0, "ip_src": "192.168.178.1", "ip_dst": "192.168.178.80", "port_src": 62894, "port_dst": 9092, "tcp_flags": "16", "ip_proto": "tcp", "tos": 0, "timestamp_start": "2019-11-17 06:08:32.453958", "timestamp_end": "1969-12-31 16:00:00.000000", "timestamp_arrival": "2019-11-17 06:08:32.453958", "packets": 1, "bytes": 52, "writer_id": "default_kafka/56419"}',
  offset: 542178,
  partition: 0,
  highWaterOffset: 542179,
  key: null }
```
需要的是其中的 value 屬性，並返回 value 中的 bytes。

4. Websocket Client（使用 Socket.IO Client）  
React 關鍵代碼：
```
state = {
    response: 0,
    endpoint: "http://127.0.0.1:3001", //server地址
    label: [],
    value: []
};

componentDidMount() {
    const { endpoint } = this.state;
    const socket = socketIOClient(endpoint);
    socket.on("broad", data => {
      // 將數據append並更新至state 
      this.setState({label: [...this.state.label, data.time]});
      this.setState({value: [...this.state.value, data.bytes]});
    });
}
```

## 附錄
#### pmacct 安裝
1. 安裝 libpcap-dev
`
$ sudo apt-get install libpcap-dev 
`
2. 安裝 librdkafka 

```
$ git clone https://github.com/edenhill/librdkafka.git
$ cd librdkafka
$ ./configure
$ make
$ make install
```
3. 安裝 libjansson-dev `$ apt-get install libpcap-dev libjansson-dev`
4. 安裝帶有選項的 pmacct 以使用 kafka  
    - 安裝依賴  
    `$ apt-get install pkg-config libtool autoconf automake bash`
    - Build GitHub code:
```
$ git clone https://github.com/pmacct/pmacct.git
$ cd pmacct
$ ./autogen.sh
$ ./configure --enable-kafka --enable-jansson --enable-ipv6
$ make
$ make install [with super-user permission]
```
5. 確認 `$ pmacctd -V`， output：

```
Promiscuous Mode Accounting Daemon, pmacctd 1.7.4-git (20191031-00)

Arguments:
 '--enable-kafka' '--enable-jansson' '--enable-ipv6' '--enable-l2' '--enable-64bit' '--enable-traffic-bins' '--enable-bgp-bins' '--enable-bmp-bins' '--enable-st-bins'

Libs:
libpcap version 1.8.1
rdkafka 1.2.2-RC1-1-g8d44dd-dirty
jansson 2.11

System:
Linux 4.15.0-20-generic #21-Ubuntu SMP Tue Apr 24 06:16:15 UTC 2018 x86_64

Compiler:
gcc 7.4.0

For suggestions, critics, bugs, contact me: Paolo Lucente <paolo@pmacct.net>.
```

#### 參考資料
- https://github.com/linsomniac/pmacct
- https://spark.apache.org/streaming/
- https://socket.io/
- https://docs.mongodb.com/
- https://www.echartsjs.com/
- https://github.com/wurstmeister/kafka-docker/issues/534
