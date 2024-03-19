import { Client, Consumer, ConsumerConfig, Message } from 'pulsar-client';
import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

export abstract class PulsarConsumer<T> implements OnModuleInit, OnModuleDestroy {
  private consumer: Consumer;
  protected readonly logger = new Logger(this.config.topic);
  protected running = true;

  protected constructor(
    private readonly pulsarClient: Client,
    private readonly config: ConsumerConfig,
  ) {}

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    this.running = false;
    await this.consumer.close();
  }

  protected async connect() {
    this.consumer = await this.pulsarClient.subscribe(this.config);
    process.nextTick(this.consume.bind(this));
    await this.consume();
  }

  private async consume() {
    while (this.running) {
      try {
        const messages = await this.consumer.batchReceive();
        await Promise.allSettled(
          messages.map((message) => this.receive(message)),
        );
      } catch (err) {
        this.logger.error('Error receiving batch, ', err);
      }
    }
  }

  protected async receive(message: Message) {
    try {
      const data: T = JSON.parse(message.getData().toString());
      console.log({ data, message: message.getMessageId().toString() });
      this.handleMessage(data);
    } catch (err) {
      this.logger.error('Error consuming, ', err);
    }
    try {
      await this.consumer.acknowledge(message);
    } catch (err) {
      this.logger.error('Error acking, ', err);
    }
  }
  protected abstract handleMessage(data: T): void;
}
