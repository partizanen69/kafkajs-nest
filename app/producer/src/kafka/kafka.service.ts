import { OnModuleInit } from '@nestjs/common';
import { Logger } from '@nestjs/common';
import { Injectable } from '@nestjs/common';
import { Kafka, ProducerRecord } from 'kafkajs';
import * as fs from 'fs';

@Injectable()
export class KafkaService implements OnModuleInit {

  kafka: Kafka;

  private readonly logger = new Logger(KafkaService.name);

  async onModuleInit() {
    this.kafka = new Kafka({
      clientId: 'producer',
      brokers: ['localhost:9093'],
      retry: {
        retries: Number.MAX_VALUE,
      },
    });

    const topic = 'message-log';
    const producer = this.kafka.producer();

    await fs.promises.unlink('../check.completenes.txt')
      .catch(err => {});

    await producer.connect();
    this.logger.verbose(`Producer connected to Kafka`);

    await this.createTopicsIfNeeded();

    let i = 0;
    const total: number = 100;

    const interval = setInterval(async () => {
      try {
        const record: ProducerRecord = {
          topic,
          messages: [
            {
              key: String(i),
              value: "this is message " + i,
            },
          ],
        };
        await producer.send(record);
        this.logger.verbose(`Succesfully sent record: ${JSON.stringify(record)}`);
      }
      catch(err) {
        this.logger.error(`Could not send the message`, err.stack);
      }
      
      i++;
      if (i === total) {
        this.logger.verbose(`Reached total messages of ${total} to be ` 
          + `produced. Stopping`);
        clearInterval(interval);
      }

    
    }, 1000);
  }

  async createTopicsIfNeeded() {
    const admin = this.kafka.admin();
    
    await admin.connect();
    this.logger.verbose('Kafka admin connected');

    const existingTopics: string[] = await admin.listTopics();
    this.logger.verbose(`Existing kafka topics: ${existingTopics.join(', ')}`);

    // if (!existingTopics.some((topic) => topic === topics.create.event)) {
    //   await this.admin.createTopics({
    //     waitForLeaders: true,
    //     topics: [
    //       { topic: topics.create.event }
    //     ]
    //   })
    // }
    
    await admin.disconnect()
    this.logger.verbose(`Kafka admin disconnected`);
  }
}