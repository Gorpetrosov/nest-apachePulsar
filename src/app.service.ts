import { Injectable } from '@nestjs/common';
import { PulsarProducerService } from './pulsar/pulsar-producer.service';
import { TEST_TOPIC } from './app.constants';

@Injectable()
export class AppService {
  constructor(private pulsarProducerService: PulsarProducerService) {}

  async sendMessage(request: any) {
    for (let i = 0; i <= 100; i++) {
      await this.pulsarProducerService.produce(TEST_TOPIC, request);
    }
  }
}
