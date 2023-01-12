import {
  Kafka,
  logCreator,
  logLevel,
  Message,
  Producer,
  ProducerBatch,
  TopicMessages,
  ProducerRecord,
  KafkaConfig
} from 'kafkajs'

interface CustomMessageFormat {
  a: string
}

export interface MessageProducer {
  connect(): Promise<void>
  handle(message: any): Promise<void>
  disconnect(): Promise<void>
}
export default class KafkaProducer implements MessageProducer {
  private producer: Producer
  private config: KafkaConfig

  constructor (args: KafkaConfig) {
    this.config = args

    const kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers
    })

    this.producer = kafka.producer()
  }

  public async connect (): Promise<void> {
    try {
      await this.producer.connect()
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }

  public async handle (message: ProducerRecord): Promise<void> {
    try {
      await this.producer.send(message)
    } catch (error) {
      console.log('Error producing message: ', error)
    }
  }

  public async handleBatch (
    messages: Array<CustomMessageFormat>
  ): Promise<void> {
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

    try {
      await this.producer.sendBatch(batch)
    } catch (error) {
      console.log('Error producing batch message: ', error)
    }
  }

  public async disconnect (): Promise<void> {
    try {
      await this.producer.disconnect()
    } catch (error) {
      console.log('Error on disconnecting producer: ', error)
    }
  }
}
