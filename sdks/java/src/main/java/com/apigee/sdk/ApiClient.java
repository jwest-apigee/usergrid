package com.apigee.sdk;

import org.apache.http.impl.client.cache.CacheConfig;

/**
 * Created by ApigeeCorporation on 7/22/15.
 */
public class ApiClient {
  private OAuthAuthenticationStrategy authenticationStrategy;
  private HttpCompliantCache cachingStrategy;

  public void setAuthenticationStrategy(OAuthAuthenticationStrategy authenticationStrategy) {
    this.authenticationStrategy = authenticationStrategy;
  }

  public void setCachingStrategy(HttpCompliantCache cachingStrategy) {
    this.cachingStrategy = cachingStrategy;
  }
}
