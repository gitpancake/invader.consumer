import { z } from 'zod';

// Configuration schemas with validation
const DatabaseConfigSchema = z.object({
  pool: z.object({
    max: z.number().min(1).max(100).default(20),
    min: z.number().min(0).default(2),
    idle: z.number().min(1000).default(10000),
    acquire: z.number().min(1000).default(30000),
    evict: z.number().min(1000).default(1000)
  }),
  retries: z.object({
    max: z.number().min(0).default(3),
    delay: z.number().min(100).default(2000)
  })
});

const ProcessingConfigSchema = z.object({
  concurrency: z.number().min(1).max(20).default(3),
  batchSize: z.number().min(10).max(1000).default(100),
  rateLimit: z.number().min(1).max(1000).default(250),
  timeout: z.number().min(5000).max(300000).default(60000),
  retries: z.number().min(0).max(5).default(3),
  gcThreshold: z.number().min(0.5).max(0.95).default(0.85)
});

const MonitoringConfigSchema = z.object({
  enabled: z.boolean().default(true),
  interval: z.number().min(1000).max(300000).default(30000),
  retention: z.number().min(60000).default(86400000), // 24 hours
  alerts: z.object({
    memory: z.object({
      warning: z.number().min(0.5).max(1).default(0.8),
      critical: z.number().min(0.5).max(1).default(0.9)
    }),
    errorRate: z.object({
      warning: z.number().min(0).max(100).default(5),
      critical: z.number().min(0).max(100).default(10)
    }),
    responseTime: z.object({
      warning: z.number().min(1000).default(15000),
      critical: z.number().min(1000).default(30000)
    })
  })
});

const ProxyConfigSchema = z.object({
  enabled: z.boolean().default(false),
  rotation: z.boolean().default(true),
  healthCheck: z.boolean().default(true),
  timeout: z.number().min(5000).max(60000).default(20000)
});

const PinataConfigSchema = z.object({
  timeout: z.number().min(5000).max(180000).default(60000),
  retries: z.number().min(0).max(5).default(3),
  retryDelay: z.number().min(1000).max(10000).default(2000)
});

const AppConfigSchema = z.object({
  environment: z.enum(['development', 'production', 'test']).default('development'),
  database: DatabaseConfigSchema,
  processing: ProcessingConfigSchema,
  monitoring: MonitoringConfigSchema,
  proxy: ProxyConfigSchema,
  pinata: PinataConfigSchema
});

export type AppConfig = z.infer<typeof AppConfigSchema>;
export type DatabaseConfig = z.infer<typeof DatabaseConfigSchema>;
export type ProcessingConfig = z.infer<typeof ProcessingConfigSchema>;
export type MonitoringConfig = z.infer<typeof MonitoringConfigSchema>;

class ConfigManager {
  private config: AppConfig;
  private readonly configOverrides: Map<string, any> = new Map();

  constructor() {
    this.config = this.loadConfiguration();
    this.validateConfiguration();
  }

  /**
   * Load configuration from environment variables
   */
  private loadConfiguration(): AppConfig {
    const rawConfig = {
      environment: process.env.NODE_ENV || 'development',
      database: {
        pool: {
          max: this.parseNumber(process.env.DB_POOL_MAX, 20),
          min: this.parseNumber(process.env.DB_POOL_MIN, 2),
          idle: this.parseNumber(process.env.DB_POOL_IDLE, 10000),
          acquire: this.parseNumber(process.env.DB_POOL_ACQUIRE, 30000),
          evict: this.parseNumber(process.env.DB_POOL_EVICT, 1000)
        },
        retries: {
          max: this.parseNumber(process.env.DB_RETRY_MAX, 3),
          delay: this.parseNumber(process.env.DB_RETRY_DELAY, 2000)
        }
      },
      processing: {
        concurrency: this.parseNumber(process.env.CONSUMER_CONCURRENCY, 3),
        batchSize: this.parseNumber(process.env.BATCH_SIZE, 100),
        rateLimit: this.parseNumber(process.env.CONSUMER_RATE_LIMIT, 250),
        timeout: this.parseNumber(process.env.PROCESSING_TIMEOUT, 60000),
        retries: this.parseNumber(process.env.PROCESSING_RETRIES, 3),
        gcThreshold: this.parseFloat(process.env.GC_THRESHOLD, 0.85)
      },
      monitoring: {
        enabled: this.parseBoolean(process.env.MONITORING_ENABLED, true),
        interval: this.parseNumber(process.env.MONITORING_INTERVAL, 30000),
        retention: this.parseNumber(process.env.MONITORING_RETENTION, 86400000),
        alerts: {
          memory: {
            warning: this.parseFloat(process.env.MEMORY_WARNING_THRESHOLD, 0.8),
            critical: this.parseFloat(process.env.MEMORY_CRITICAL_THRESHOLD, 0.9)
          },
          errorRate: {
            warning: this.parseNumber(process.env.ERROR_RATE_WARNING, 5),
            critical: this.parseNumber(process.env.ERROR_RATE_CRITICAL, 10)
          },
          responseTime: {
            warning: this.parseNumber(process.env.RESPONSE_TIME_WARNING, 15000),
            critical: this.parseNumber(process.env.RESPONSE_TIME_CRITICAL, 30000)
          }
        }
      },
      proxy: {
        enabled: this.parseBoolean(process.env.PROXY_ENABLED, !!process.env.PROXY_LIST),
        rotation: this.parseBoolean(process.env.PROXY_ROTATION, true),
        healthCheck: this.parseBoolean(process.env.PROXY_HEALTH_CHECK, true),
        timeout: this.parseNumber(process.env.PROXY_TIMEOUT, 20000)
      },
      pinata: {
        timeout: this.parseNumber(process.env.PINATA_TIMEOUT, 60000),
        retries: this.parseNumber(process.env.PINATA_RETRIES, 3),
        retryDelay: this.parseNumber(process.env.PINATA_RETRY_DELAY, 2000)
      }
    };

    return rawConfig as AppConfig;
  }

  /**
   * Validate configuration against schema
   */
  private validateConfiguration(): void {
    try {
      this.config = AppConfigSchema.parse(this.config);
      console.log(`[ConfigManager] Configuration validated successfully (env: ${this.config.environment})`);
    } catch (error) {
      console.error('[ConfigManager] Configuration validation failed:', error);
      throw new Error('Invalid configuration');
    }
  }

  /**
   * Get the full configuration
   */
  get(): AppConfig {
    return this.config;
  }

  /**
   * Get database configuration
   */
  getDatabase(): DatabaseConfig {
    return this.config.database;
  }

  /**
   * Get processing configuration
   */
  getProcessing(): ProcessingConfig {
    return this.config.processing;
  }

  /**
   * Get monitoring configuration
   */
  getMonitoring(): MonitoringConfig {
    return this.config.monitoring;
  }

  /**
   * Get environment-specific configuration
   */
  getEnvironment(): 'development' | 'production' | 'test' {
    return this.config.environment;
  }

  /**
   * Check if running in production
   */
  isProduction(): boolean {
    return this.config.environment === 'production';
  }

  /**
   * Check if running in development
   */
  isDevelopment(): boolean {
    return this.config.environment === 'development';
  }

  /**
   * Override a configuration value (for testing)
   */
  override(path: string, value: any): void {
    this.configOverrides.set(path, value);
    
    // Apply override to current config
    const keys = path.split('.');
    let current: any = this.config;
    
    for (let i = 0; i < keys.length - 1; i++) {
      if (!(keys[i] in current)) {
        current[keys[i]] = {};
      }
      current = current[keys[i]];
    }
    
    current[keys[keys.length - 1]] = value;
    
    console.log(`[ConfigManager] Configuration override: ${path} = ${value}`);
  }

  /**
   * Clear all overrides
   */
  clearOverrides(): void {
    this.configOverrides.clear();
    this.config = this.loadConfiguration();
    this.validateConfiguration();
    console.log('[ConfigManager] Configuration overrides cleared');
  }

  /**
   * Get configuration summary for logging
   */
  getSummary(): Record<string, any> {
    return {
      environment: this.config.environment,
      concurrency: this.config.processing.concurrency,
      batchSize: this.config.processing.batchSize,
      rateLimit: this.config.processing.rateLimit,
      dbPoolMax: this.config.database.pool.max,
      monitoringEnabled: this.config.monitoring.enabled,
      proxyEnabled: this.config.proxy.enabled
    };
  }

  /**
   * Update configuration from environment changes
   */
  reload(): void {
    console.log('[ConfigManager] Reloading configuration from environment...');
    this.config = this.loadConfiguration();
    this.validateConfiguration();
  }

  // Utility parsing functions
  private parseNumber(value: string | undefined, defaultValue: number): number {
    if (!value) return defaultValue;
    const parsed = parseInt(value, 10);
    return isNaN(parsed) ? defaultValue : parsed;
  }

  private parseFloat(value: string | undefined, defaultValue: number): number {
    if (!value) return defaultValue;
    const parsed = parseFloat(value);
    return isNaN(parsed) ? defaultValue : parsed;
  }

  private parseBoolean(value: string | undefined, defaultValue: boolean): boolean {
    if (!value) return defaultValue;
    return value.toLowerCase() === 'true' || value === '1';
  }
}

// Export singleton instance
export const configManager = new ConfigManager();

// Environment validation
export function validateEnvironment(): void {
  const required = [
    'DATABASE_URL',
    'RABBITMQ_URL',
    'RABBITMQ_QUEUE',
    'PINATA_API_KEY',
    'PINATA_API_SECRET'
  ];

  const missing = required.filter(key => !process.env[key]);
  
  if (missing.length > 0) {
    console.error('[ConfigManager] Missing required environment variables:', missing);
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  console.log('[ConfigManager] Required environment variables validated');
}

// Configuration presets
export const configPresets = {
  development: {
    'processing.concurrency': 2,
    'processing.batchSize': 50,
    'processing.rateLimit': 100,
    'monitoring.interval': 60000,
    'database.pool.max': 10
  },
  
  production: {
    'processing.concurrency': 5,
    'processing.batchSize': 300,
    'processing.rateLimit': 250,
    'monitoring.interval': 30000,
    'database.pool.max': 20
  },
  
  highVolume: {
    'processing.concurrency': 10,
    'processing.batchSize': 500,
    'processing.rateLimit': 500,
    'database.pool.max': 50
  }
};

export function applyPreset(presetName: keyof typeof configPresets): void {
  const preset = configPresets[presetName];
  if (!preset) {
    throw new Error(`Unknown configuration preset: ${presetName}`);
  }

  Object.entries(preset).forEach(([path, value]) => {
    configManager.override(path, value);
  });

  console.log(`[ConfigManager] Applied configuration preset: ${presetName}`);
}