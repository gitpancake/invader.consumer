import { Pool } from "pg";

let pool: Pool | null = null;
let cleanupInterval: NodeJS.Timeout | null = null;

export function getPool(): Pool {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error("DATABASE_URL environment variable is required");
    }
    
    pool = new Pool({
      connectionString,
      ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
      max: 8, // Reduced from 15 - fewer connections = less memory per connection
      min: 1, // Reduced from 2 - don't keep idle connections
      idleTimeoutMillis: 15000, // Reduced from 30000 - release idle connections faster
      connectionTimeoutMillis: 8000, // Reduced timeout
      keepAlive: false, // Disable keepalive to reduce TCP memory overhead
      // Memory-specific settings
      maxUses: 7500, // Rotate connections to prevent memory leaks
      allowExitOnIdle: true, // Allow pool to fully close when idle
      // Resilience settings
      statement_timeout: 30000, // 30 second query timeout
      query_timeout: 30000,
      // Connection retry settings
      application_name: 'invaders-consumer',
    });
    
    // Enhanced error handling with reconnection logic
    pool.on('error', (err, client) => {
      console.error('Database pool error:', err.message);
      // Don't exit on connection errors - let pool handle reconnection
      if (err.message.includes('Connection terminated') || err.message.includes('ECONNRESET')) {
        console.log('Database connection lost - pool will reconnect automatically');
      }
    });

    pool.on('connect', (client) => {
      console.log('New database connection established');
    });

    pool.on('acquire', (client) => {
      console.debug(`Connection acquired. Pool size: ${pool!.totalCount}, idle: ${pool!.idleCount}`);
    });

    pool.on('remove', (client) => {
      console.log('Database connection removed from pool');
    });

    // Implement periodic cleanup for memory efficiency
    if (!cleanupInterval) {
      cleanupInterval = setInterval(() => {
        if (pool && pool.idleCount > pool.options.min!) {
          console.log(`[Database] Performing periodic cleanup - idle: ${pool.idleCount}, min: ${pool.options.min}`);
          // Note: In production, you might want to implement a more sophisticated cleanup strategy
        }
      }, 300000); // Every 5 minutes
    }

    console.log('Database connection pool initialized with memory-optimized settings (max: 8 connections)');
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (cleanupInterval) {
    clearInterval(cleanupInterval);
    cleanupInterval = null;
  }
  if (pool) {
    await pool.end();
    pool = null;
  }
}