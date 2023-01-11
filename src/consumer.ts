import {
  Consumer,
  ConsumerSubscribeTopic,
  EachBatchPayload,
  Kafka,
  EachMessagePayload
} from 'kafkajs'
import { config } from './config'
import { KafkaType } from './producer'

export default class ConsumerFactory {
  private kafkaConsumer: Consumer

  public constructor (args: KafkaType) {
    const kafka = new Kafka({
      clientId: args.clientId,
      brokers: args.brokers
    })
    this.kafkaConsumer = kafka.consumer({ groupId: args.groupId! })
  }

  public async start (): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: config.kafka.TOPIC,
      fromBeginning: false
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)

      await this.kafkaConsumer.run({
        eachMessage: async (messagePayload: EachMessagePayload) => {

          const { topic, partition, message } = messagePayload
          const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
          console.log(`consumer- ${prefix} ${message.key}#${message.value}`)

          const jsonObj = JSON.parse(message.value.toString())

          if(jsonObj.bodyTemperature >37){
            console.log("---alert", jsonObj)
          }

        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async startBatchConsumer (): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: config.kafka.TOPIC,
      fromBeginning: false
    }
    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)
      await this.kafkaConsumer.run({
        eachBatch: async (eachBatchPayload: EachBatchPayload) => {
          const { batch } = eachBatchPayload
          for (const message of batch.messages) {
            const prefix = `${batch.topic}[${batch.partition} | ${message.offset}] / ${message.timestamp}`
            // if(message.value.bodyTemperature > 37){

            // }
            console.log(`consumer- ${prefix} ${message.key}#${message.value}`)
            console.log('---prefix---')
            console.log('prefix', prefix)
            console.log('---k---')
            console.log('---message.key---', message.key)
            console.log('---k---')
            console.log('---v---')
            console.log('message.value', message.value)
            console.log('---v---')
          }
        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async shutdown (): Promise<void> {
    await this.kafkaConsumer.disconnect()
  }
}
