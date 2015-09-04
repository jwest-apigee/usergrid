import com.fasterxml.jackson.databind.JsonNode;
import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.UsergridClient;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.UsergridResponse;

import javax.script.ScriptException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by ayeshadastagiri on 6/29/15.
 */
public class ConnectionEx {

    public static void TraverseGraph(String namePerson) throws ScriptException {

        //Suggestions of whom to follow are given to the 'namePerson'
        // It depends on 1) People who visit the same restaurant as him/her 2) People who they follow follow someone else

        List<String> AlreadyFollowing = new ArrayList<String>();
        List<String> VisitedRest = new ArrayList<String>();




    }

    public static void main(String[] args) throws ScriptException {
        Properties props = new Properties();
        FileInputStream f = null;

        try {
            f = new FileInputStream("/Users/ayeshadastagiri/incubator-usergrid/sdks/java/src/main/resources/example.properties");
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

        Usergrid.initialize(apiUrl, orgName, appName);
        UsergridClient client = Usergrid.getInstance();

        UsergridResponse response = client.authorizeAppClient(client_id, client_secret);

        System.out.println(response);

        String token = client.getAccessToken();

        System.out.println(token);

        UsergridEntity person1 = new UsergridEntity();
        UsergridEntity person2 = new UsergridEntity();
        UsergridEntity person3 = new UsergridEntity();
        UsergridEntity person4 = new UsergridEntity();

        person1.changeType("people");
        person1.setProperty("name", "A");
        person1.setProperty("age", 20);
        person1.PUT();

        person2.changeType("people");
        person2.setProperty("name", "B");
        person2.setProperty("age", 21);
        person2.PUT();

        person3.changeType("people");
        person3.setProperty("name", "C");
        person3.setProperty("age", 22);
        person3.PUT();

        person4.changeType("people");
        person4.setProperty("name", "D");
        person4.setProperty("age", 23);
        person4.PUT();



        UsergridEntity rest1 = new UsergridEntity();
        UsergridEntity rest2 = new UsergridEntity();
        UsergridEntity rest3 = new UsergridEntity();
        UsergridEntity rest4 = new UsergridEntity();



        rest1.changeType("Restaurants");
        rest1.setProperty("name", "Amici");
        rest1.setProperty("Location", "San Jose");
        rest1.PUT();

        rest2.changeType("Restaurants");
        rest2.setProperty("name", "Yard House");
        rest2.setProperty("Location", "LA");
        rest2.PUT();

        rest3.changeType("Restaurants");
        rest3.setProperty("name", "PizzaStudio");
        rest3.setProperty("Location", "LA");
        rest3.PUT();

        rest4.changeType("Restaurants");
        rest4.setProperty("name", "CPK");
        rest4.setProperty("Location", "San Jose");
        rest4.PUT();


        //http://localhost:8080/test-organization/internapp/people/A/visits/restaurants/Amici?
        // access_token=YWMtBX_YHB6LEeWuZCl2OudNNwAAAU5klcheA1VJ-Ex4ZISkBIh3EEA_5XkvYh8

        UsergridResponse c1 = client.connectEntities(person1, rest1, "Visits");
        UsergridResponse c2 =client.connectEntities(person3, rest1, "Visits");
        UsergridResponse c3 = client.connectEntities(person2, person1, "Follows");
        UsergridResponse c4 = client.connectEntities(person1, person3, "Follows");
        UsergridResponse c5 = client.connectEntities(person3, person4, "Follows");

        Map<String, JsonNode> t = person1.getProperties();

        System.out.println("test : " +  t.get("name"));

        JsonNode personName = t.get("name");


        System.out.println(c1.getLastEntity().getStringProperty("name"));

        System.out.println(c1.getTimestamp()); // returns timestamp
        UsergridEntity usergridEntity1 = c3.getEntities().get(0); // returns timestamp

        System.out.println(usergridEntity1.getStringProperty("name"));
        System.out.println(usergridEntity1.getProperties().get("metadata").get("connecting").get("follows"));

        System.out.println(c3.getTimestamp());

//        System.out.println(c1.getFirstEntity());

        try {
            TraverseGraph("A");
        } catch (ScriptException e) {
            e.printStackTrace();
        }


    }

}
