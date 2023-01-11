import ConsumerFactory from './consumer'
import ProducerFactory from './producer'
import { messages } from './messages'
import { ProducerRecord } from 'kafkajs'
import {config} from './config'

console.log("config",config)

async function main () {
  const producer = new ProducerFactory({
    brokers: config.kafka.BROKERS,
    clientId: config.kafka.CLIENTID
  })

  const consumer = new ConsumerFactory({
    brokers: config.kafka.BROKERS,
    clientId: config.kafka.CLIENTID,
    groupId: config.kafka.GROUPID
  })

  await producer.start()

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

    await producer.sendMessage(payloads)
  }, 5000)

  await consumer.start()
}

main().catch(err => {
  console.error(err)
})
