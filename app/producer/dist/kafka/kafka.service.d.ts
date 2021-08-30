import { OnModuleInit } from '@nestjs/common';
import { Kafka } from 'kafkajs';
export declare class KafkaService implements OnModuleInit {
    kafka: Kafka;
    private readonly logger;
    onModuleInit(): Promise<void>;
    createTopicsIfNeeded(): Promise<void>;
}
