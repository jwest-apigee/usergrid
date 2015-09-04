package org.apache.usergrid.java.client;

import org.apache.usergrid.java.client.response.UsergridResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 9/2/15.
 */
public class Usergrid {

  private static final Map<String, UsergridClient> instances_;
  public static final String STR_DEFAULT = "default";

  static {

    instances_ = new HashMap<>(5);
    instances_.put(STR_DEFAULT, new UsergridClient());
  }

  public static UsergridClient getInstance() {

    return getInstance(STR_DEFAULT);
  }

  public static UsergridClient getInstance(String id) {

    UsergridClient client = instances_.get(id);

    if (client == null) {
      client = new UsergridClient();
      instances_.put(id, client);
    }

    return client;
  }

  public static void initialize(String apiUrl, String orgName, String appName) {

    UsergridClient client = getInstance(STR_DEFAULT);
    client.withApiUrl(apiUrl)
        .withOrganizationId(orgName)
        .withApplicationId(appName);
  }


  public static RequestBuilder collection(String cats) {
    RequestBuilder builder = new RequestBuilder();
    builder.collection = cats;
    return builder;
  }

  public static UsergridResponse GET(String type, String uriSuffix) {
    return getInstance().GET(type, uriSuffix);
//    RequestBuilder builder = new RequestBuilder();
//    builder.collection = type;
//    builder.uriSuffix = uriSuffix;
//    return builder.GET();
  }
}