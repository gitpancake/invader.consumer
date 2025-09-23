import { Pool } from "pg";

let pool: Pool | null = null;

export function getPool(): Pool {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error("DATABASE_URL environment variable is required");
    }
    
    pool = new Pool({
      connectionString,
      ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
      max: 15, // Reduced to prevent overwhelming Railway
      min: 2, // Maintain minimum connections
      idleTimeoutMillis: 30000, // Shorter idle timeout for Railway
      connectionTimeoutMillis: 10000, // Longer connection timeout
      keepAlive: true, 
      keepAliveInitialDelayMillis: 0, // Start keepalive immediately
      // Resilience settings for Railway
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

    pool.on('remove', (client) => {
      console.log('Database connection removed from pool');
    });
    
    console.log('Database connection pool initialized with resilience settings (max: 15 connections)');
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}