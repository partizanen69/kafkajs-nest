"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var KafkaService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaService = void 0;
const common_1 = require("@nestjs/common");
const common_2 = require("@nestjs/common");
const kafkajs_1 = require("kafkajs");
const fs = require("fs");
const PORT = +process.env.PORT;
const taskDuration = +process.env.TASK_DURATION;
if (!taskDuration) {
    throw new Error(`Please set the task duration`);
}
const sessionTimeout = 15000;
let isTaskRunning = false;
let KafkaService = KafkaService_1 = class KafkaService {
    constructor() {
        this.logger = new common_1.Logger(KafkaService_1.name);
    }
    async onModuleInit() {
        this.logger.verbose(`sessionTimeout is ${sessionTimeout}.`
            + ` Task duration is ${taskDuration}`);
        if (taskDuration > sessionTimeout) {
            this.logger.warn(`Warning! Task duration is longer than sessionTimeout`);
        }
        const groupId = 'consumer';
        const clientId = `${groupId}-${PORT}`;
        const kafka = new kafkajs_1.Kafka({
            clientId,
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
            eachBatchAutoResolve: false,
            autoCommitThreshold: 1,
            partitionsConsumedConcurrently: 1,
            eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, uncommittedOffsets, isRunning, isStale, }) => {
                var _a;
                for (const message of batch.messages) {
                    console.log(`Message in a batch: `, message.key.toString());
                }
                const uncommitedOffsets = uncommittedOffsets();
                console.log(uncommitedOffsets.topics, (_a = uncommitedOffsets.topics[0]) === null || _a === void 0 ? void 0 : _a.partitions);
                const message = batch.messages[0];
                if (!message) {
                    this.logger.warn(`There is no sigle message in the batch`);
                    return;
                }
                const key = message.key.toString();
                this.logger.verbose(`Message ${key} start processing`);
                if (isTaskRunning) {
                    this.logger.error(`Concurrent processing identified`);
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
                try {
                    await new Promise(resolve => setTimeout(resolve, taskDuration));
                    this.logger.verbose(`Message ${key} processed`);
                }
                catch (err) {
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
        });
    }
    async doHeartbeat(heartbeat) {
        try {
            await heartbeat();
        }
        catch (err) {
            this.logger.warn(`Heartbeat failed because of: ${err.message}`);
        }
    }
};
KafkaService = KafkaService_1 = __decorate([
    common_2.Injectable()
], KafkaService);
exports.KafkaService = KafkaService;
//# sourceMappingURL=kafka.service.js.map