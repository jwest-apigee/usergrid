import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.UsergridClient;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.query.QueryResult;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.response.UsergridResponse;

import java.util.HashMap;
//import org.apache.usergrid.java.client.query


/**
 * Created by ApigeeCorporation on 6/26/15.
 */
public class JeffTest {

  public static final String APP_CLIENT_ID = "YXA6WMhAuFJTEeWoggrRE9kXrQ";
  public static final String APP_CLIENT_SECRET = "YXA6_Nn__Xvx8rEwsYaKgEm5SWFwtJ0";
  public static final String USERGRID_URL = "https://api-connectors-prod.apigee.net/appservices";
  public static final String ORG_NAME = "api-connectors";
  public static final String APP_NAME = "sdksandbox";
  public static final String COLLECTION = "Morepet";

  public static void main(String[] args) {

    // below is the sample code
    Usergrid.initialize(USERGRID_URL, ORG_NAME, APP_NAME);

    // if you want to have direct access, you can get an instance of the singleton
    UsergridClient client = Usergrid.getInstance();

    UsergridResponse r = client.authorizeAppClient(APP_CLIENT_ID, APP_CLIENT_SECRET);

    System.out.println(r.getAccessToken());

    // new query builder
    UsergridQuery q = null;

    UsergridEntity e = Usergrid.GET("pets", "max").first();
    e.PUT();

    QueryResult qr = null;

    for (int x = 0; x < 20; x++) {
      UsergridEntity pet = new UsergridEntity(COLLECTION);
      pet.setProperty("name", "pet-" + x);
      pet.setProperty("age", x);
      pet.setLocation(-1, -2);
      pet.PUT();
    }

    try{
      Thread.sleep(1000);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    System.out.println("GETTING by query...");

    // new query builder
    q = new UsergridQuery.Builder()
        .collection(COLLECTION)
        .limit(500)
        .locationWithin(5.0, -1, -20)
        .build();

    // singleton operation
    qr = Usergrid.getInstance().GET(q);

    boolean keepGoing = true;

    while (keepGoing) {

      for (UsergridEntity entity : qr) {
        System.out.println(entity);
      }

      if (qr.hasMorePages())
        qr = qr.retrieveNextPage();
      else
        keepGoing = false;
    }

    HashMap<String, Object> update = new HashMap<>();

    update.put("JeffWest", "WuzHere");
//
//    System.out.println("PUT!");
//    QueryResult rput = q.PUT(update);
//
//    System.out.println("SIzE: " + rput.getEntities().size());

    System.out.println("DELETE!");
    QueryResult rdel = q.DELETE();

    System.out.println("SIzE: " + rdel.getEntities().size());

    keepGoing = true;

    while (keepGoing) {

      for (UsergridEntity entity : rdel) {
        System.out.println(entity);
      }

      if (rdel.hasMorePages())
        rdel = rdel.retrieveNextPage();
      else
        keepGoing = false;
    }
  }
}
