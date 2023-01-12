// import KafkaConsumer from './consumer'
// import KafkaProducer from './producer'
import { messages } from './messages'
import { ProducerRecord } from 'kafkajs'
import { config } from './config'
import KafkaFactory from './kafka-factory'

async function main () {
  //   const producer = new KafkaProducer({
  //     brokers: config.kafka.BROKERS,
  //     clientId: config.kafka.CLIENTID
  //   })

  //   const consumer = new KafkaConsumer({
  //     brokers: config.kafka.BROKERS,
  //     clientId: config.kafka.CLIENTID,
  //     groupId: config.kafka.GROUPID,
  //     topic: config.kafka.TOPIC
  //   })

  const kafkaFactory = new KafkaFactory({
    brokers: config.kafka.BROKERS,
    clientId: config.kafka.CLIENTID,
    groupId: config.kafka.GROUPID,
    topic: config.kafka.TOPIC
  })

  const producer = kafkaFactory.producer()
  const consumer = kafkaFactory.consumer()

  await producer.connect()

  let i = 0

  setInterval(async function () {
    i = i >= messages.length - 1 ? 0 : i + 1
    const payloads: ProducerRecord = {
      topic: config.kafka.TOPIC,
      messages: [
        { key: 'coronavirus-alert', value: JSON.stringify(messages[i]) }
      ]
    }
    console.log('payloads=', payloads)

    await producer.handle(payloads)
  }, 5000)

  await consumer.connect()
  await consumer.startConsumer()
}

main().catch(err => {
  console.error(err)
})
