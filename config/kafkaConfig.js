const { Kafka } = require('kafkajs')
//client id, username, password, broker
const {
    KAFKA_CLIENT_ID,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
    KAFKA_BROKER
} = require("./index")

const optionsSasl = {
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD,
    mechanism: "plain"
}

const KAFKAClient_TH = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: [KAFKA_BROKER],
    ssl: true,
    sasl: optionsSasl,
    retry: 3
})

module.exports = {
    KafkaClient: KAFKAClient_TH
}