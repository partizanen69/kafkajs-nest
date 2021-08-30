import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

if (!process.env.PORT) {
  throw new Error(`Port to run the app is not specified`);
}
const PORT = +process.env.PORT;

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(PORT);
  console.log(`Listening on port ${PORT}`);
}
bootstrap();
