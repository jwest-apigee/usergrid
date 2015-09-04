package com.apigee.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.cache.CacheResponseStatus;
import org.apache.http.client.cache.HttpCacheContext;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.usergrid.java.client.response.UsergridResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/22/15.
 */
public class ApiClient {

  private static final String HTTP_POST = "POST";
  private static final String STR_BLANK = "";

  public static class Features {
    boolean enableHttpCache = false;
  }

  private OAuthAuthenticationStrategy authenticationStrategy;
  private HttpCompliantCache cachingStrategy;
  private String id;
  private String urlBase;
  private HttpClient client;
  private Features features;
  private ObjectMapper objectMapper;

  public ApiClient(String id, String urlBase, Features f) {
    this.id = id;
    this.features = f;
    this.urlBase = urlBase;
    objectMapper = new ObjectMapper();

    CacheConfig cacheConfig = CacheConfig.custom()
        .setMaxCacheEntries(1000)
        .setMaxObjectSize(8192)
        .build();

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(30000)
        .setSocketTimeout(30000)
        .build();

    CachingHttpClientBuilder clientBuilder = CachingHttpClients.custom();
    clientBuilder.setDefaultRequestConfig(requestConfig);

    if (this.features.enableHttpCache) {
      clientBuilder.setCacheConfig(cacheConfig);
    }

    client = clientBuilder.build();

  }


  public UsergridResponse apiRequest(final String method,
                                final Map<String, Object> params,
                                final Object data,
                                final String... segments) throws IOException {

    HttpCacheContext context = HttpCacheContext.create();
    HttpGet httpget = new HttpGet("https://api-connectors-prod.apigee.net/appservices/api-connectors/sandbox/pets/f623086a-2b2a-11e5-8675-191dbc7d9cbc");

    HttpResponse response = client.execute(httpget, context);

    UsergridResponse myObject = objectMapper.readValue(response.getEntity().getContent(), UsergridResponse.class);

    CacheResponseStatus responseStatus = context.getCacheResponseStatus();

    switch (responseStatus) {

      case CACHE_HIT:
        System.out.println("A response was generated from the cache with " +
            "no requests sent upstream");
        break;

      case CACHE_MODULE_RESPONSE:
        System.out.println("The response was generated directly by the " +
            "caching module");
        break;

      case CACHE_MISS:
        System.out.println("The response came from an upstream server");
        break;

      case VALIDATED:
        System.out.println("The response was generated from the cache " +
            "after validating the entry with the origin server");
        break;
    }

    return myObject;
  }

  public static void main(String[] args) {
    Features f = new Features();
    f.enableHttpCache = true;

    ApiClient client = new ApiClient("foo", "bar", f);

    try {
      UsergridResponse response = client.apiRequest(null, null, null, "/");
      System.out.println(response);
      client.apiRequest(null, null, null, "/");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}

