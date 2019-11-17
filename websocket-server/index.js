const express = require('express')
const http = require('http')
const socketIO = require('socket.io')

const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({
    kafkaHost: '192.168.178.80:9092'
});
const consumer = new Consumer(
    client,
    [{
        topic: "realTimeChart",
        fromOffset: 'lastest'
    }], {
        autoCommit: true
    }
)

// our localhost port
const port = 4001

const app = express()

// our server instance
const server = http.createServer(app)

// This creates our socket using the instance of the server
const io = socketIO(server)

// socket.io 
io.on('connection', socket => {
    console.log('User connected')

    socket.on('disconnect', () => {
        console.log('user disconnected')
    })

    consumer.on('message', (message) => {
        console.log(message);
        if (message.value != "realTimeChart") {
            const netFlowMag = JSON.parse(message.value);
            const res = {
                time: netFlowMag.timestamp_arrival.substring(11,19),
                bytes: netFlowMag.bytes
            }
            socket.emit('broad', res);
        }
    })
})

server.listen(port, () => console.log(`Listening on port ${port}`))