import * as http from "http";
import * as https from "https";
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';
import axios from 'axios';

interface ProxyConfig {
  host: string;
  port: number;
  protocol: 'http' | 'https';
  auth?: {
    username: string;
    password: string;
  };
}

export class ProxyRotator {
  private proxies: ProxyConfig[] = [];
  private workingProxies: ProxyConfig[] = [];
  private currentProxyIndex: number = 0;
  private failedProxies: Set<string> = new Set();
  private proxyFailureCount: Map<string, number> = new Map();
  private proxyHealthStatus: Map<string, { working: boolean; lastChecked: number; consecutiveFailures: number }> = new Map();
  private isOxylabs: boolean = false;
  private healthCheckInProgress: boolean = false;

  constructor() {
    this.loadProxiesFromEnv();
    // Set up periodic re-checking every 10 minutes
    if (this.proxies.length > 0) {
      setInterval(() => {
        this.recheckFailedProxies();
      }, 600000); // 10 minutes
    }
  }

  public async initialize(): Promise<void> {
    if (this.proxies.length > 0 && !this.healthCheckInProgress) {
      // For OxLabs, skip health checks since they handle failures internally and may have domain-specific auth
      if (this.isOxylabs) {
        console.log(`[ProxyRotator] 🔗 OxLabs detected - skipping health checks (they handle failures internally)`);
        this.workingProxies = [...this.proxies]; // Copy all proxies to working pool
        return;
      }
      await this.performInitialHealthCheck();
    }
  }

  private loadProxiesFromEnv(): void {
    const proxyList = process.env.PROXY_LIST;
    console.log(`[ProxyRotator] Debug: PROXY_LIST env var = ${proxyList ? '[CONFIGURED]' : '[NOT FOUND]'}`);
    
    if (proxyList) {
      try {
        // Format: "http://proxy1:8080,https://user:pass@proxy2:3128" or "user:pass@proxy.com:7777"
        const proxyStrings = proxyList.split(',');
        this.proxies = proxyStrings.map(proxyStr => {
          let urlString = proxyStr.trim();
          
          // Add protocol if missing (default to https for security)
          if (!urlString.startsWith('http://') && !urlString.startsWith('https://')) {
            urlString = `https://${urlString}`;
          }
          
          const url = new URL(urlString);
          return {
            host: url.hostname,
            port: parseInt(url.port) || (url.protocol === 'https:' ? 443 : 80),
            protocol: url.protocol.replace(':', '') as 'http' | 'https',
            auth: url.username && url.password ? {
              username: url.username,
              password: url.password
            } : undefined
          };
        });
        console.log(`[ProxyRotator] Loaded ${this.proxies.length} proxies from environment`);
        
        // If using Oxylabs or DataImpulse, disable failure tracking since they handle rotation internally
        this.isOxylabs = this.proxies.some(p => 
          p.host.includes('oxylabs.io') || 
          p.host.includes('pr.oxylabs.io') || 
          p.host.includes('datacenter.oxylabs.io') ||
          p.host.includes('dataimpulse.com')
        );
        
        if (this.isOxylabs) {
          if (this.proxies.some(p => p.host.includes('oxylabs.io'))) {
            console.log('[ProxyRotator] Detected Oxylabs proxies - optimizing configuration for premium service');
          } else if (this.proxies.some(p => p.host.includes('dataimpulse.com'))) {
            console.log('[ProxyRotator] Detected DataImpulse proxies - optimizing configuration for premium service');
          }
        }
      } catch (error) {
        console.log('[ProxyRotator] Error parsing proxy list:', error);
      }
    } else {
      console.log('[ProxyRotator] No PROXY_LIST environment variable found - operating without proxy');
    }
  }

  private async performInitialHealthCheck(): Promise<void> {
    if (this.healthCheckInProgress) return;
    
    this.healthCheckInProgress = true;
    console.log(`[ProxyRotator] 🔍 Starting health check for ${this.proxies.length} proxies...`);
    
    // Clear working proxies array before health check
    this.workingProxies = [];
    
    const healthCheckPromises = this.proxies.map(async (proxy) => {
      const isHealthy = await this.checkProxyHealth(proxy);
      const proxyKey = `${proxy.host}:${proxy.port}`;
      
      this.proxyHealthStatus.set(proxyKey, {
        working: isHealthy,
        lastChecked: Date.now(),
        consecutiveFailures: isHealthy ? 0 : 1
      });
      
      if (isHealthy) {
        this.workingProxies.push(proxy);
        console.log(`[ProxyRotator] ✅ Proxy ${proxyKey} - HEALTHY`);
      } else {
        console.log(`[ProxyRotator] ❌ Proxy ${proxyKey} - FAILED`);
      }
      
      return { proxy, isHealthy };
    });
    
    await Promise.all(healthCheckPromises);
    
    console.log(`[ProxyRotator] 📊 Health check complete: ${this.workingProxies.length}/${this.proxies.length} proxies working`);
    this.healthCheckInProgress = false;
  }

  private async checkProxyHealth(proxy: ProxyConfig): Promise<boolean> {
    try {
      const proxyUrl = `${proxy.protocol}://${proxy.auth ? `${proxy.auth.username}:${proxy.auth.password}@` : ''}${proxy.host}:${proxy.port}`;
      console.log(`[ProxyRotator] 🔍 Testing proxy URL: ${proxyUrl.replace(/:([^:@]+)@/, ':***@')}`); // Mask password in logs
      
      const agent = new HttpsProxyAgent(proxyUrl);
      
      // For DataImpulse/OxyLabs proxies, test with a simpler endpoint
      const testUrl = this.isOxylabs ? 
        'https://api.space-invaders.com/flashinvaders/' : 
        'https://httpbin.org/get';
      
      const response = await axios.get(testUrl, {
        httpsAgent: agent,
        timeout: 20000, // Longer timeout for premium proxies
        validateStatus: (status) => status < 500, // Accept 4xx but not 5xx
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
      });
      
      const isHealthy = response.status < 400;
      console.log(`[ProxyRotator] 🔍 Proxy ${proxy.host}:${proxy.port} health check: status ${response.status} = ${isHealthy ? 'HEALTHY' : 'UNHEALTHY'}`);
      
      return isHealthy;
      
    } catch (error: any) {
      // Check specifically for 407 authentication errors
      const is407Error = error?.response?.status === 407 || 
                        error?.message?.includes('407') || 
                        error?.message?.includes('Proxy Authentication') ||
                        error?.message?.includes('CONNECT tunnel failed, response 407');
      
      console.log(`[ProxyRotator] 🔍 Proxy ${proxy.host}:${proxy.port} health check ERROR: ${error.message} (407: ${is407Error})`);
      
      if (is407Error) {
        console.log(`[ProxyRotator] 🚫 Proxy ${proxy.host}:${proxy.port} - 407 Authentication failure (credentials may be incorrect)`);
        return false;
      }
      
      // Log full error details for debugging
      console.log(`[ProxyRotator] 🔍 Full error details:`, {
        message: error.message,
        code: error.code,
        status: error?.response?.status,
        statusText: error?.response?.statusText
      });
      
      // For other errors, also mark as unhealthy but could be temporary
      return false;
    }
  }

  private async recheckFailedProxies(): Promise<void> {
    const failedProxies = this.proxies.filter(proxy => {
      const proxyKey = `${proxy.host}:${proxy.port}`;
      const status = this.proxyHealthStatus.get(proxyKey);
      return status && !status.working;
    });
    
    if (failedProxies.length === 0) return;
    
    console.log(`[ProxyRotator] 🔄 Re-checking ${failedProxies.length} failed proxies...`);
    
    for (const proxy of failedProxies) {
      const proxyKey = `${proxy.host}:${proxy.port}`;
      const isHealthy = await this.checkProxyHealth(proxy);
      const currentStatus = this.proxyHealthStatus.get(proxyKey)!;
      
      if (isHealthy && !currentStatus.working) {
        // Proxy is now working - add it back to working pool
        this.workingProxies.push(proxy);
        this.proxyHealthStatus.set(proxyKey, {
          working: true,
          lastChecked: Date.now(),
          consecutiveFailures: 0
        });
        console.log(`[ProxyRotator] ✅ Proxy ${proxyKey} - RECOVERED`);
      } else if (!isHealthy) {
        // Still failing
        this.proxyHealthStatus.set(proxyKey, {
          working: false,
          lastChecked: Date.now(),
          consecutiveFailures: currentStatus.consecutiveFailures + 1
        });
      }
    }
    
    console.log(`[ProxyRotator] 📊 Recheck complete: ${this.workingProxies.length}/${this.proxies.length} proxies working`);
  }

  private getNextProxy(): ProxyConfig | null {
    // Use working proxies registry if available
    if (this.workingProxies.length > 0) {
      const proxy = this.workingProxies[this.currentProxyIndex % this.workingProxies.length];
      this.currentProxyIndex = (this.currentProxyIndex + 1) % this.workingProxies.length;
      return proxy;
    }
    
    // Fallback to original logic if no working proxies (health check still in progress or all failed)
    if (this.proxies.length === 0) return null;
    
    console.log('[ProxyRotator] ⚠️ No verified working proxies available, falling back to original pool');
    
    // For Oxylabs, use simple rotation since they handle failures internally
    if (this.isOxylabs) {
      const proxy = this.proxies[this.currentProxyIndex % this.proxies.length];
      this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxies.length;
      return proxy;
    }
    
    // For other proxies, use failure tracking
    let attempts = 0;
    const maxAttempts = this.proxies.length;
    
    while (attempts < maxAttempts) {
      const proxy = this.proxies[this.currentProxyIndex % this.proxies.length];
      const proxyKey = `${proxy.host}:${proxy.port}`;
      this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxies.length;
      
      // Skip proxies that have failed too many times
      const failureCount = this.proxyFailureCount.get(proxyKey) || 0;
      if (failureCount < 3) { // Allow up to 3 failures before skipping
        return proxy;
      }
      
      attempts++;
    }
    
    // If all proxies have failed too many times, reset failure counts and try again
    if (attempts >= maxAttempts) {
      console.log('[ProxyRotator] All proxies have failed multiple times, resetting failure counts');
      this.proxyFailureCount.clear();
      this.failedProxies.clear();
      return this.proxies[0]; // Return first proxy as fallback
    }
    
    return null;
  }

  private markProxyAsFailed(proxy: ProxyConfig, error?: Error): void {
    // Don't track failures for Oxylabs since they handle rotation internally and can be spotty
    if (this.isOxylabs) {
      // Silent failure tracking for OxLabs - no logging to reduce spam
      return;
    }
    
    const proxyKey = `${proxy.host}:${proxy.port}`;
    const currentCount = this.proxyFailureCount.get(proxyKey) || 0;
    
    // Check for 407 Proxy Authentication errors - blacklist immediately
    const is407Error = error?.message?.includes('407') || error?.message?.includes('Proxy Authentication');
    
    if (is407Error) {
      // Set failure count high enough to permanently blacklist
      this.proxyFailureCount.set(proxyKey, 999);
      this.failedProxies.add(proxyKey);
      console.log(`[ProxyRotator] 🚫 Proxy ${proxyKey} BLACKLISTED - 407 Authentication failure`);
    } else {
      // Normal failure tracking
      this.proxyFailureCount.set(proxyKey, currentCount + 1);
      this.failedProxies.add(proxyKey);
      console.log(`[ProxyRotator] Proxy ${proxyKey} failed (${currentCount + 1} times)`);
    }
  }

  public createProxyAgent(targetUrl: string): { agent: any; proxy: ProxyConfig | null } {
    const proxy = this.getNextProxy();
    
    if (!proxy) {
      // Return undefined agent for direct connection
      return { agent: undefined, proxy: null };
    }

    const proxyUrl = `${proxy.protocol}://${proxy.auth ? `${proxy.auth.username}:${proxy.auth.password}@` : ''}${proxy.host}:${proxy.port}`;
    
    let agent: any;
    if (targetUrl.startsWith('https://')) {
      agent = new HttpsProxyAgent(proxyUrl);
    } else {
      agent = new HttpProxyAgent(proxyUrl);
    }

    return { agent, proxy };
  }

  public handleProxyFailure(proxy: ProxyConfig | null, error?: Error): void {
    if (proxy) {
      this.markProxyAsFailed(proxy, error);
    }
  }

  public hasProxies(): boolean {
    return this.workingProxies.length > 0;
  }

  public getProxyCount(): number {
    return this.workingProxies.length;
  }

  public getTotalProxyCount(): number {
    return this.proxies.length;
  }
}

// Export singleton instance
export const proxyRotator = new ProxyRotator();