import { Module } from '@nestjs/common';
import { KafkaService } from './kafka.service';

@Module({
  imports: [],
  controllers: [],
  providers: [
    KafkaService,
  ],
})
export class KafkaModule {

}