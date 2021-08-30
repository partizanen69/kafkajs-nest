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
let KafkaService = KafkaService_1 = class KafkaService {
    constructor() {
        this.logger = new common_1.Logger(KafkaService_1.name);
    }
    async onModuleInit() {
        this.kafka = new kafkajs_1.Kafka({
            clientId: 'producer',
            brokers: ['localhost:9093'],
            retry: {
                retries: Number.MAX_VALUE,
            },
        });
        const topic = 'message-log';
        const producer = this.kafka.producer();
        await fs.promises.unlink('../check.completenes.txt')
            .catch(err => { });
        await producer.connect();
        this.logger.verbose(`Producer connected to Kafka`);
        await this.createTopicsIfNeeded();
        let i = 0;
        const total = 100;
        const interval = setInterval(async () => {
            try {
                const record = {
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
            catch (err) {
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
        const existingTopics = await admin.listTopics();
        this.logger.verbose(`Existing kafka topics: ${existingTopics.join(', ')}`);
        await admin.disconnect();
        this.logger.verbose(`Kafka admin disconnected`);
    }
};
KafkaService = KafkaService_1 = __decorate([
    common_2.Injectable()
], KafkaService);
exports.KafkaService = KafkaService;
//# sourceMappingURL=kafka.service.js.map