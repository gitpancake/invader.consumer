import * as http from "http";
import * as https from "https";
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';

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
  private currentProxyIndex: number = 0;
  private failedProxies: Set<string> = new Set();
  private proxyFailureCount: Map<string, number> = new Map();
  private isOxylabs: boolean = false;

  constructor() {
    this.loadProxiesFromEnv();
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
        
        // If using Oxylabs, disable failure tracking since they handle rotation internally
        this.isOxylabs = this.proxies.some(p => 
          p.host.includes('oxylabs.io') || 
          p.host.includes('pr.oxylabs.io') || 
          p.host.includes('datacenter.oxylabs.io')
        );
        
        if (this.isOxylabs) {
          console.log('[ProxyRotator] Detected Oxylabs proxies - optimizing configuration for premium service');
        }
      } catch (error) {
        console.log('[ProxyRotator] Error parsing proxy list:', error);
      }
    } else {
      console.log('[ProxyRotator] No PROXY_LIST environment variable found - operating without proxy');
    }
  }

  private getNextProxy(): ProxyConfig | null {
    if (this.proxies.length === 0) return null;
    
    // For Oxylabs, use simple rotation since they handle failures internally
    if (this.isOxylabs) {
      const proxy = this.proxies[this.currentProxyIndex];
      this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxies.length;
      return proxy;
    }
    
    // For other proxies, use failure tracking
    let attempts = 0;
    const maxAttempts = this.proxies.length;
    
    while (attempts < maxAttempts) {
      const proxy = this.proxies[this.currentProxyIndex];
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
    // Don't track failures for Oxylabs since they handle rotation internally
    if (this.isOxylabs) {
      console.log(`[ProxyRotator] Request failed with Oxylabs proxy ${proxy.host}:${proxy.port} - will retry with same endpoint`);
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
    return this.proxies.length > 0;
  }

  public getProxyCount(): number {
    return this.proxies.length;
  }
}

// Export singleton instance
export const proxyRotator = new ProxyRotator();