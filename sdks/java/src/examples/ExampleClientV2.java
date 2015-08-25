import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.query.QueryResult;
import org.apache.usergrid.java.client.response.ApiResponse;
//import org.apache.usergrid.java.client.query

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ApigeeCorporation on 6/26/15.
 */
public class ExampleClientV2 {

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
    //*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****
    //*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****//*****

    // ignore above...

    // below is the sample code
    Usergrid.initialize(apiUrl, orgName, appName);

    // if you want to have direct access, you can get an instance of the singleton
    Usergrid client = Usergrid.getInstance();

    Usergrid brandon = Usergrid.getInstance("Brandon's App");
    brandon.withApplicationId("Brandon");

    Usergrid jeff = Usergrid.getInstance("Jeff's App");
    Usergrid robert = Usergrid.getInstance("Robert's App");

    UsergridEntity jeffCat = new UsergridEntity("pet");
    jeffCat.setProperty("name", "max")
        .setProperty("age", 15)
        .setProperty("weight", 21)
        .setProperty("owner", (String) null);

    // these functions will use the singleton client instance
    jeffCat.save(); // PUT if by name/uuid, otherwise POST
    jeffCat.post(); // POST to default client to create, fails if exists?
    jeffCat.put(); // PUT to default client to update, fails if doesn't exist?
    jeffCat.delete(); // DELETE to default client

    //you can also use a named instance of the usergrid client, but it is not required
    jeff.save(jeffCat);
    jeff.post(jeffCat);
    jeff.put(jeffCat);
    jeff.delete(jeffCat);

    UsergridEntity owner = new UsergridEntity();
    owner.changeType("owner");
    owner.setProperty("name", "jeff");
    owner.setProperty("age", 15);
    owner.save();

    // new function to create connections (singleton instance)
    owner.connect(jeffCat, "owns");

    // updated legacy function on client instance
    jeff.connectEntities(jeffCat, owner, "ownedBy");
    jeff.connectEntities(owner, jeffCat, "owns");

    // new query builder
    UsergridQuery q = new UsergridQuery.Builder()
        .collection("pets")
        .limit(100)
        .gt("age", 100)
        .gte("age", 100)
        .containsWord("field", "value")
        .desc("cats")
        .asc("dogs")
        .build();

    // singleton operation
    QueryResult qr = q.get();

    UsergridEntity e1 = q.get().first();

    Map<String, Object> fields = new HashMap<>();

    // singleton operation
    ApiResponse r = q.put(fields);

    // direct client operation
    r = jeff.put(q, fields);
  }
}
