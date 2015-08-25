import com.apigee.sdk.*;
import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.query.UsergridQuery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

//import org.apache.usergrid.java.client.query

/**
 * Created by ApigeeCorporation on 6/26/15.
 */
public class ApigeeExample1 {

  public static void main(String[] args) {
    Properties props = new Properties();
    FileInputStream f = null;

    try {
      f = new FileInputStream("/Users/ApigeeCorporation/code/usergrid/myfork/incubator-usergrid/sdks/java/src/main/resources/secure/api-connectors.properties");
      props.load(f);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    String orgName = props.getProperty("usergrid.organization");
    String appName = props.getProperty("usergrid.application");

    String client_id = props.getProperty("usergrid.client_id");
    String client_secret = props.getProperty("usergrid.client_secret");
    String apiUrl = props.getProperty("usergrid.apiUrl");

    // ignore above...

    // below is the sample code


    Usergrid ugClient = ApigeeSDK.initUsergridClient(apiUrl, orgName, appName);

    ApiClient apiClient1 = ApigeeSDK.initApiClient("MyApi1");
    ApiClient apiClient2 = ApigeeSDK.initApiClient("MyOtherAPI");

    InsightsClient insightsClient = ApigeeSDK.initInsightsClient("MyOtherAPI");

    HashMap<String, Object> properties = new HashMap<String, Object>();
    properties.put("DefaultClientId", "Jeff");
    properties.put("DefaultClientSecret", "Jeff");
    properties.put("DefaultApiKey", "Jeff");
    properties.put("DefaultAPIClient", apiClient1);

//    ApigeeSDK.defaults("CachingStrategy", new HttpCompliantCache("name"));

    ApigeeSDK.initialize(properties);

    String authorizationURL = "https://www.example.com/oauth2/authorize",
        tokenURL = "https://www.example.com/oauth2/token",
        clientID = "EXAMPLE_CLIENT_ID",
        clientSecret = "EXAMPLE_CLIENT_SECRET",
        callbackURL = "http://localhost:3000/auth/example/callback";


//    apiClient1.setAuthenticationStrategy(new OAuthAuthenticationStrategy(
//        authorizationURL,
//        tokenURL,
//        clientID,
//        clientSecret,
//        callbackURL
//    ));
//
//    apiClient1.setCachingStrategy(new HttpCompliantCache("name"));
//
//    ApigeeSDK.ApiClient("MyApi1").setCachingStrategy(new HttpCompliantCache("name"));


//    Client client = Usergrid.getInstance();
//
//    ApiResponse response = client.authorizeAppClient(client_id, client_secret);

//    System.out.println(response);
//
//    String token = client.getAccessToken();
//
//    System.out.println(token);

    Usergrid.initialize(apiUrl, orgName, appName);

    Usergrid brandon = Usergrid.getInstance("Brandon's App");
    Usergrid jeff = Usergrid.getInstance("Jeff's App");
    Usergrid robert  = Usergrid.getInstance("Robert's App");


    UsergridEntity jeffCat = new UsergridEntity("pet");
    jeffCat.setProperty("name", "max");
    jeffCat.setProperty("age", 15);
    jeffCat.setProperty("weight", 21);
    jeffCat.setProperty("owner", (String) null);
    jeffCat.save(); // PUT if by name/uuid, otherwise POST

    UsergridEntity brandonCat =  UsergridEntity.copyOf(jeffCat);

    jeffCat.post(); // POST to default client to create, fails if exists?
    jeffCat.put(); // PUT to default client to update, fails if doesn't exist?
    jeffCat.delete(); // DELETE to default client
//    pet.patch(); // PATCH to update individual fields?


    UsergridEntity owner = new UsergridEntity();
    owner.changeType("owner");
    owner.setProperty("name", "jeff");
    owner.setProperty("age", 15);
    owner.save();

    owner.connect(jeffCat, "owns");

    // consider for v2 api
    //    /_entities/{collection}:{name}
    //    /_entities/{uuid}


//    client.connectEntities(pet, owner, "ownedBy");
//    client.connectEntities(owner, pet, "owns");

    UsergridQuery q = new UsergridQuery.Builder()
        .collection("pets")
        .limit(100)
        .gt("age", 100)
        .gte("age", 100)
        .containsWord("field", "value")
        .desc("cats")
        .asc("dogs")
        .build();

    q.get();
  }
}
