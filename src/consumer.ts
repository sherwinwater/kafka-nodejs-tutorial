import {
  Consumer,
  ConsumerSubscribeTopic,
  EachBatchPayload,
  Kafka,
  EachMessagePayload,
  KafkaConfig,
  KafkaMessage
} from 'kafkajs'

export interface MessageConsumer {
  connect(): Promise<void>
  handle(message: any): Promise<void>
  disconnect(): Promise<void>
}

export default class KafkaConsumer implements MessageConsumer {
  private consumer: Consumer
  private config: KafkaConfig & { groupId: string; topic: string }

  constructor (args: KafkaConfig & { groupId: string; topic: string }) {
    this.config = args

    const kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers
    })
    this.consumer = kafka.consumer({ groupId: this.config.groupId })
  }

  async connect (): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: this.config.topic,
      fromBeginning: false
    }

    try {
      await this.consumer.connect()
      await this.consumer.subscribe(topic)
    } catch (error) {
      console.log('Error on connecting consumer ', error)
    }
  }

  async startConsumer (): Promise<void> {
    try {
      await this.consumer.run({
        eachMessage: payload => this.handle(payload)
      })
    } catch (error) {
      console.log('Error on startConsumer ', error)
    }
  }

  async startBatchConsumer (): Promise<void> {
    try {
      await this.consumer.run({
        eachBatch: payload => this.handleBatch(payload)
      })
    } catch (error) {
      console.log('Error on startBatchConsumer ', error)
    }
  }

  async handle ({
    topic,
    partition,
    message
  }: EachMessagePayload): Promise<void> {
    try {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`consumer- ${prefix} ${message.key}#${message.value}`)

      const jsonObj = JSON.parse(message.value.toString())

      if (jsonObj.bodyTemperature > 37) {
        console.log('---alert', jsonObj)
        console.log('-------')
        this.logMessageHandler(message)
      }
    } catch (error) {
      console.log('Error on consuming message ', error)
    }
  }

  async handleBatch ({ batch }: EachBatchPayload): Promise<void> {
    try {
      for (const message of batch.messages) {
        const prefix = `${batch.topic}[${batch.partition} | ${message.offset}] / ${message.timestamp}`
        console.log(`consumer- ${prefix} ${message.key}#${message.value}`)
      }
    } catch (error) {
      console.log('Error on consuming batch message ', error)
    }
  }

  logMessageHandler (message: KafkaMessage) {
    console.log({
      value: message.value?.toString()
    })
  }

  async disconnect (): Promise<void> {
    try {
      await this.consumer.disconnect()
    } catch (error) {
      console.log('Error on disconnecting consumer ', error)
    }
  }
}
