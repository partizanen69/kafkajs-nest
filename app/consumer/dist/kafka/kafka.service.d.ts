import { OnModuleInit } from '@nestjs/common';
export declare class KafkaService implements OnModuleInit {
    private readonly logger;
    onModuleInit(): Promise<void>;
    doHeartbeat(heartbeat: () => Promise<void>): Promise<void>;
}
