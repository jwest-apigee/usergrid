package com.apigee.sdk;

import org.apache.http.impl.client.cache.CacheConfig;

/**
 * Created by ApigeeCorporation on 7/22/15.
 */
public class HttpCompliantCache {
  private String name;
  private CacheConfig cacheConfig = CacheConfig.custom()
      .setMaxCacheEntries(1000)
      .setMaxObjectSize(8192)
      .build();


  public HttpCompliantCache(String name) {
    this.name = name;
  }
}
