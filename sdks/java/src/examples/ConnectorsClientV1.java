import org.apache.usergrid.java.client.Client;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.UsergridResponse;

/**
 * Created by ApigeeCorporation on 6/26/15.
 */
public class  ConnectorsClientV1 {

  public static void main(String[] args) {

    String client_id = "b3U6NtTDevNkEeSJyjdhYz1UDg";
    String client_secret = "b3U6VRK0UK18UQFunLyAbPz2ahNvAsA";

    Client client = new Client()
        .withApiUrl("https://api-connectors-prod.apigee.net/appservices")
        .withOrganizationId("api-connectors")
        .withApplicationId("blueprints");

    UsergridResponse response = client.authorizeAppClient(client_id, client_secret);

    System.out.println(response);

    String token = client.getAccessToken();

    System.out.println(token);

    UsergridEntity e = new UsergridEntity("pet");

    e.setProperty("name", "max");
    e.setProperty("age", 15);
    e.setProperty("owner", "jeff");

    UsergridResponse entityResponse = client.createEntity(e);

    System.out.println(entityResponse);
  }
}
