import { Request, Response } from "express";
import { MongoClient, MongoUrl } from '../db/mongodb';
export function getAccessTrend(req: Request, res: Response) {

  MongoClient.connect(MongoUrl, function(err, db) {
    // res.setHeader("Content-Type", "application/json");
    if (err) throw err;
    var dbo = db.db("kafka-netflow");

    let pipeline = [
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

    // console.log(pipeline)

    dbo
      .collection("netflow")
      .aggregate(pipeline, (err, cursor) => {
        cursor.toArray((err, data) => {
          res.json(data);
          // res.json(protocolData);
        });
      });
  });
} 