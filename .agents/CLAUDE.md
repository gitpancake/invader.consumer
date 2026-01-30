# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Project Overview

Space Invaders Consumer is a Node.js service that processes Space Invaders game data through a message queue pipeline:

1. Consumes flash game objects from RabbitMQ
2. Downloads associated images from the Space Invaders API
3. Uploads images to IPFS via Pinata for decentralized storage
4. Updates PostgreSQL database with the resulting IPFS CIDs

## Architecture

```
RabbitMQ → EnhancedFlashConsumer → Download Image → Upload to Pinata/IPFS → Batch Update PostgreSQL
```

Key directories:
- `src/enhanced-consumer.ts` - Main consumer orchestrator (recommended entry point)
- `src/index.ts` - Legacy/simpler consumer
- `src/util/rabbitmq/` - Message queue base consumer
- `src/util/database/` - Connection pooling & queries
- `src/util/batch-updater/` - Efficient bulk DB updates
- `src/util/circuit-breaker/` - Fault tolerance (open/closed/half-open states)
- `src/util/concurrent/` - Parallel processing engine
- `src/util/memory/` - Automatic GC & memory optimization
- `src/util/monitoring/` - Metrics collection & health checks
- `src/util/proxy/` - Proxy rotation for downloads
- `src/util/rate-limiter/` - API request throttling
- `src/util/config/` - Zod-validated configuration

## Common Commands

```bash
npm run build          # Compile TypeScript to dist/
npm start              # Run compiled consumer with GC flag
npm run dev            # Development with file watching (nodemon)
npm test               # Test mode (messages requeued, not removed)
npm run performance-check  # Analyze system performance
npm run health-check   # Verify system health
npm run metrics        # Export performance metrics
```

## Environment Variables

**Required:**
- `DATABASE_URL` - PostgreSQL connection string
- `RABBITMQ_URL` - RabbitMQ connection URL
- `RABBITMQ_QUEUE` - Queue name to consume
- `PINATA_JWT` (or `PINATA_API_KEY` + `PINATA_API_SECRET`) - Pinata authentication

**Optional performance tuning:**
- `CONSUMER_CONCURRENCY` - 1-20 (default: 3)
- `BATCH_SIZE` - 10-1000 (default: 100)
- `CONSUMER_RATE_LIMIT` - 1-1000 req/min (default: 250)
- `GC_THRESHOLD` - 0.5-0.95 (default: 0.85)
- `DB_POOL_MIN`/`DB_POOL_MAX` - Connection pool sizing

**Optional proxy (for high volume or rate-limited scenarios):**
- `PROXY_LIST` - Comma-separated proxy URLs (e.g., `http://proxy1:8080,user:pass@proxy2:3128`)

Note: AWS credentials (AWS_REGION, BUCKET_NAME, etc.) are NOT required - the AWS SDK dependency is unused.

## Database Schema

The `flashes` table contains:
- `flash_id` (primary key)
- `img` (image path/URL)
- `city`, `text`, `player`, `timestamp`, `flash_count` (metadata)
- `ipfs_cid` (IPFS content identifier - NULL until processed)

## Key Patterns

- **Circuit breakers** protect against cascading failures (image download, IPFS upload)
- **Batch updates** flush to database every 5 seconds or when batch is full
- **Memory optimizer** triggers GC at configurable heap threshold
- **Graceful shutdown** on SIGINT/SIGTERM waits for active requests
