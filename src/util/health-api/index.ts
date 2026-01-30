import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

export async function startHealthApi(): Promise<void> {
  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn'],
  });

  app.enableCors({
    origin: process.env.DASHBOARD_URL || 'http://localhost:3000',
    credentials: true,
  });

  const port = process.env.HEALTH_API_PORT || 3002;
  await app.listen(port);
  console.log(`[HealthAPI] Running on port ${port}`);
}
