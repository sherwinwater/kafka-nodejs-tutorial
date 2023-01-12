import KafkaProducer from './producer'
import KafkaConsumer from './consumer'
import { KafkaConfig } from 'kafkajs'

export interface MessagingFactory {
  producer(configuration: ProducerConfiguration): KafkaProducer
  consumer(configuration: ConsumerConfiguration): KafkaConsumer
}

type ProducerConfiguration = KafkaConfig
type ConsumerConfiguration = KafkaConfig & { groupId: string; topic: string }

export default class KafkaFactory implements MessagingFactory {
  private config: ConsumerConfiguration

  constructor (args: any) {
    this.config = args
  }

  producer (): KafkaProducer {
    return new KafkaProducer({
      clientId: this.config.clientId,
      brokers: this.config.brokers
    })
  }

  consumer (): KafkaConsumer {
    return new KafkaConsumer({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
      groupId: this.config.groupId,
      topic: this.config.topic
    })
  }
}
