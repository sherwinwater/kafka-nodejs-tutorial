import {
  Kafka,
  logCreator,
  logLevel,
  Message,
  Producer,
  ProducerBatch,
  TopicMessages,
  ProducerRecord
} from 'kafkajs'

interface CustomMessageFormat {
  a: string
}

export type KafkaType = {
  brokers: string[]
  clientId: string
  groupId?: string
}

export default class ProducerFactory {
  private producer: Producer

  constructor (args: KafkaType) {
    const kafka = new Kafka({
      clientId: args.clientId,
      brokers: args.brokers
    })

    this.producer = kafka.producer()
  }

  public async start (): Promise<void> {
    try {
      await this.producer.connect()
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }

  public async shutdown (): Promise<void> {
    await this.producer.disconnect()
  }

  public async sendMessage (message: ProducerRecord): Promise<void> {
    await this.producer.send(message)
  }

  public async sendBatch (messages: Array<CustomMessageFormat>): Promise<void> {
    const kafkaMessages: Array<Message> = messages.map(message => {
      return {
        value: JSON.stringify(message)
      }
    })

    const topicMessages: TopicMessages = {
      topic: 'producer-topic',
      messages: kafkaMessages
    }

    const batch: ProducerBatch = {
      topicMessages: [topicMessages]
    }

    await this.producer.sendBatch(batch)
  }
}
