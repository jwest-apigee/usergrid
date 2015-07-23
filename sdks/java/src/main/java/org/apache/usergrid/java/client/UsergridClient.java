package org.apache.usergrid.java.client;
//
//import org.apache.usergrid.java.client.model.UGCollection;
//import org.apache.usergrid.java.client.model.UGObject;

/**
 * Created by ApigeeCorporation on 6/23/15.
 */
public class UsergridClient {

  private static final UsergridClient instance = new UsergridClient();


  public static UsergridClient withApplicationId(String appId)
  {
    return instance.withApplicationId(appId);
  }

  //    private JerseyClient restClient;
  private String baseURL;
  private String orgName;
  private String appName;

  public UsergridClient() {
  }

//  public UGCollection collection(String name) {
//    UGCollection col = new UGCollection(name, this);
//    return col;
//  }

  private UsergridClient(String baseURL, String orgName, String appName) {
    this.baseURL = baseURL;
    this.orgName = orgName;
    this.appName = appName;

//        restClient = new JerseyClient();
  }

//  public UGResult create(UGObject object) {
//    return null;
//  }

//  public UGResult put(UGObject ugObject) {
//    return null;
//  }

//  public UGResult post(UGObject ugObject) {
//    // jersey client
//    return null;
//  }

//  public UGResult get(UGObject ugObject) {
//    return null;
//  }

//  public UGResult delete(UGObject ugObject) {
//    return null;
//  }

  public Object entityBuilder() {
    return null;
  }
}
