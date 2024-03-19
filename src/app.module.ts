import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PulsarModule } from './pulsar/pulsar.module';
import { ConfigModule } from '@nestjs/config';
import { AppConsumer } from './app.consumer';

@Module({
  imports: [ConfigModule.forRoot(), PulsarModule],
  controllers: [AppController],
  providers: [AppService, AppConsumer],
})
export class AppModule {}
