"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = require("@nestjs/core");
const app_module_1 = require("./app.module");
if (!process.env.PORT) {
    throw new Error(`Port to run the app is not specified`);
}
const PORT = +process.env.PORT;
async function bootstrap() {
    const app = await core_1.NestFactory.create(app_module_1.AppModule);
    await app.listen(PORT);
    console.log(`Listening on port ${PORT}`);
}
bootstrap();
//# sourceMappingURL=main.js.map