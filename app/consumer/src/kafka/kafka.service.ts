import { OnModuleInit } from '@nestjs/common';
import { Logger } from '@nestjs/common';
import { Injectable } from '@nestjs/common';
import { Kafka, OffsetsByTopicPartition } from 'kafkajs';
import * as fs from 'fs';

const PORT: number = +process.env.PORT;
const taskDuration: number = +process.env.TASK_DURATION;
if (!taskDuration) {
  throw new Error(`Please set the task duration`);
}
const sessionTimeout: number = 15000;

let isTaskRunning: boolean = false;

@Injectable()
export class KafkaService implements OnModuleInit {

  private readonly logger = new Logger(KafkaService.name);

  async onModuleInit() {
    this.logger.verbose(`sessionTimeout is ${sessionTimeout}.` 
      + ` Task duration is ${taskDuration}`);
    if (taskDuration > sessionTimeout) {
      this.logger.warn(`Warning! Task duration is longer than sessionTimeout`);
    }

    const groupId: string = 'consumer';
    const clientId = `${groupId}-${PORT}`;
    
    const kafka = new Kafka({
      clientId,
      // brokers: ['kafka:9093'] // this works as well
      brokers: ['localhost:9093'],
      retry: {
        retries: Number.MAX_VALUE,
      },
    });

    const topic = 'message-log';
    const consumer = kafka.consumer({ 
      groupId,
      sessionTimeout,
      maxInFlightRequests: 1,
      // heartbeatInterval: sessionTimeout * 0.7,
    });

    await consumer.connect();
    this.logger.verbose(`${clientId} connected to Kafka`);

    const { COMMIT_OFFSETS } = consumer.events;
    consumer.on(COMMIT_OFFSETS, () => {
      this.logger.verbose(`COMMIT_OFFSETS`);
    });

    await consumer.subscribe({ 
      topic,
      fromBeginning: true,
    });

    await consumer.run({
      eachBatchAutoResolve: false, // need to call resolveOffset for processed message
      autoCommitThreshold: 1, // commit after each processed message
      partitionsConsumedConcurrently: 1, // consume only from single partition
      eachBatch: async ({
          batch,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
          uncommittedOffsets,
          isRunning,
          isStale,
      }) => {
        
        for (const message of batch.messages) {
          console.log(`Message in a batch: `, message.key.toString());
        }

        const uncommitedOffsets: OffsetsByTopicPartition = uncommittedOffsets();
        console.log(
          uncommitedOffsets.topics, 
          uncommitedOffsets.topics[0]?.partitions
          );
        
        const message = batch.messages[0];
        if (!message) {
          this.logger.warn(`There is no sigle message in the batch`);
          return;
        }

        const key = message.key.toString();
        this.logger.verbose(`Message ${key} start processing`);

        if (isTaskRunning) {
          this.logger.error(`Concurrent processing identified`);
          // skip message can be done here since eachBatchAutoResolve === false
        }

        isTaskRunning = true;
        consumer.pause([{ topic }]);

        let timer = setInterval(async () => {
          if (!isTaskRunning) {
            clearInterval(timer);
            timer = null;
            return;
          }
          this.logger.verbose(`Heartbeat for message ${key}`);
          await this.doHeartbeat(heartbeat);
        }, 5000);
        
        // emulate long running asynchronous task
        try {
          await new Promise(resolve => setTimeout(resolve, taskDuration));
          this.logger.verbose(`Message ${key} processed`);
        }
        catch(err) {
          this.logger.warn(`Message ${key} has been processed ` 
            + `with error: ${err.message}`);
        }
        
        isTaskRunning = false;
        clearInterval(timer);

        
        resolveOffset(message.offset);
        this.logger.verbose(`Message ${key} offset resolver`);
        await commitOffsetsIfNecessary();
        this.logger.verbose(`Message ${key} commit if necessary`);

        await fs.promises.appendFile('../check.completenes.txt', `${PORT}-${key}\n`);

        await this.doHeartbeat(heartbeat);
        consumer.resume([{ topic }]);
      },
    })

  }

  async doHeartbeat(heartbeat: () => Promise<void>): Promise<void> {
    // it fails during rebalance
    try {
      await heartbeat();
    }
    catch(err) {
      this.logger.warn(`Heartbeat failed because of: ${err.message}`);
    } 
  }
}