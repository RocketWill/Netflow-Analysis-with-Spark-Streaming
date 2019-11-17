export const MongoClient = require("mongodb").MongoClient;
export const MongoUrl = process.env.MONGO_HOST || "mongodb://192.168.178.80:27017";