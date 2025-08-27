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
      max: 20, // Increased from 10 to handle burst traffic
      idleTimeoutMillis: 60000, // Increased from 30s to 60s
      connectionTimeoutMillis: 5000, // Increased from 2s to 5s
      keepAlive: true, // Keep connections alive
      keepAliveInitialDelayMillis: 10000,
    });
    
    // Add error handling for the pool
    pool.on('error', (err) => {
      console.error('Database pool error:', err);
    });
    
    pool.on('connect', () => {
      console.log('Database connection established');
    });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}