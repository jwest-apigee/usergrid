package com.apigee.sdk;

import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.UsergridClient;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by ApigeeCorporation on 7/22/15.
 */
public class ApigeeSDK {

  static {
    apiClients_ = new HashMap<String, ApiClient>();
//    apiClients_.put("_UsergridDefault", new ApiClient());
  }

  private static Map<String, ApiClient> apiClients_;

  public static UsergridClient initUsergridClient(String apiUrl, String orgName, String appName) {
    Usergrid.initialize(apiUrl, orgName, appName);
    return Usergrid.getInstance();
  }

  public static ApiClient initApiClient(String id) {
    ApiClient client = apiClients_.get(id);

    if (client == null) {
      client = new ApiClient(id, null, new ApiClient.Features());
      apiClients_.put(id, client);
    }

    return client;
  }

  public static InsightsClient initInsightsClient(String myOtherAPI) {
    return null;
  }

  public static void initialize(HashMap properties) {

  }

  public static ApiClient ApiClient(String id) {
    ApiClient client = apiClients_.get(id);

    if (client == null) {
      client = new ApiClient(id, null, new ApiClient.Features());
      apiClients_.put(id, client);
    }

    return client;
  }

  public static void defaultCachingStrategy(HttpCompliantCache name) {

  }
}
