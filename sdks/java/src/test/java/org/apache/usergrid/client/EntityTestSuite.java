package org.apache.usergrid.client;

import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.response.UsergridResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by ApigeeCorporation on 9/10/15.
 */
public class EntityTestSuite {

  @Before
  public void before() {
    Usergrid.initialize(SDKTestConfiguration.USERGRID_URL, SDKTestConfiguration.ORG_NAME, SDKTestConfiguration.APP_NAME);
    Usergrid.authorizeAppClient(SDKTestConfiguration.APP_CLIENT_ID, SDKTestConfiguration.APP_CLIENT_SECRET);
  }

  @Test
  public void testEntityCreationSuccess() {
    String collectionName = "ect" + System.currentTimeMillis();

    Map<String, Map<String, String>> entityMap = new HashMap<>(7);

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    entityMap.put("redsquare", fields);

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, "redsquare", fields);

//    entity has been created
  }

  @Test
  public void testCollectionNameLength() {
    String collectionName = "testCollectionNameLength" + System.currentTimeMillis();
    collectionName += collectionName;
    collectionName += collectionName;
    collectionName += collectionName;
    collectionName += collectionName;
    collectionName += collectionName;
    collectionName += collectionName;
    collectionName += collectionName;

    Map<String, Map<String, String>> entityMap = new HashMap<>(7);

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    entityMap.put("redsquare", fields);

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, "redsquare", fields);

    UsergridEntity eLookup = UsergridEntity.GET(collectionName, "redsquare");

    assertTrue("The returned entity is null!", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));
  }


  @Test
  public void testDuplicateEntityNameFailure() {
    String collectionName = "testDuplicateEntityNameFailure" + System.currentTimeMillis();

    Map<String, Map<String, String>> entityMap = new HashMap<>(7);

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    entityMap.put("redsquare", fields);

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, "redsquare", fields);

    e.POST(); //should work

    UsergridResponse r = e.POST(); //should fail

    assertTrue("Second entity create should not succeed!", r.getError() != null);
  }

  @Test
  public void testEntityLookupByName() {
    String collectionName = "testEntityLookupByName" + System.currentTimeMillis();

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    String entityName = "redsquare";

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, entityName, fields);

    UsergridEntity eLookup = UsergridEntity.GET(collectionName, entityName);

    assertTrue("The returned entity is null!", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));
  }

  @Test
  public void testEntityLookupByUUID() {
    String collectionName = "testEntityLookupByUUID" + System.currentTimeMillis();

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    String entityName = "redsquare";

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, entityName, fields);

    UsergridEntity eLookup = UsergridEntity.GET(collectionName, e.getUuidString());

    assertTrue("The returned entity is null!", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));
  }

  @Test
  public void testEntityLookupByQuery() {
    String collectionName = "testEntityLookupByQuery" + System.currentTimeMillis();

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    String entityName = "redsquare";

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, entityName, fields);

    UsergridQuery q;
    UsergridEntity eLookup;

    SDKTestUtils.indexSleep();

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("color", "red").build();

    eLookup = q.GET().first();
    assertTrue("The entity was not returned on lookup", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("name", entityName).build();

    eLookup = q.GET().first();

    assertTrue("The entity was not returned on lookup", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("shape", "square").build();

    eLookup = q.GET().first();

    assertTrue("The entity was not returned on lookup", eLookup != null);
    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("shape", "circle").build();

    eLookup = q.GET().first();

    assertTrue("The entity was not expected to be returned on lookup", eLookup == null);
  }

  @Test
  public void testEntityUpdate() {
    String collectionName = "testEntityLookupByUUID" + System.currentTimeMillis();

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");
    fields.put("orientation", "up");

    String entityName = "redsquare";

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, entityName, fields);

    SDKTestUtils.sleep(1000);

    UsergridQuery q;
    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("orientation", "up").build();

    UsergridEntity eLookup;

    eLookup = q.GET().first();

    assertTrue("The returned entity does not have the same UUID when querying by field", e.getUuidString().equals(eLookup.getUuidString()));

    e.setProperty("orientation", "down");

    e.PUT();


    eLookup = UsergridEntity.GET(collectionName, e.getUuidString());

    assertTrue("The returned entity does not have the same UUID", e.getUuidString().equals(eLookup.getUuidString()));
    assertTrue("The field was not updated!", eLookup.getStringProperty("orientation").equals("down"));

    SDKTestUtils.sleep(1000);

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("orientation", "up").build();

    eLookup = q.GET().first();

    assertTrue("The entity was returned for old value!", eLookup == null);
  }

  @Test
  public void testEntityDelete() {
    String collectionName = "testEntityDelete" + System.currentTimeMillis();

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");
    fields.put("orientation", "up");

    String entityName = "redsquare";

    UsergridEntity e = SDKTestUtils.createEntity(collectionName, entityName, fields);

    SDKTestUtils.indexSleep();

    UsergridQuery q;
    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("orientation", "up").build();

    UsergridEntity eLookup;
    eLookup = q.GET().first();

    assertTrue("The returned entity was null!", eLookup != null);
    assertTrue("The returned entity does not have the same UUID when querying by field", e.getUuidString().equals(eLookup.getUuidString()));

    e.DELETE();

    eLookup = UsergridEntity.GET(collectionName, e.getUuidString());

    assertTrue("The entity was not expected to be returned by UUID", eLookup == null);

    eLookup = UsergridEntity.GET(collectionName, e.getName());

    assertTrue("The entity was not expected to be returned by name", eLookup == null);

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("color", "red")
        .build();

    eLookup = q.GET().first();

    assertTrue("The entity was not expected to be returned", eLookup == null);

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("shape", "square")
        .build();

    eLookup = q.GET().first();

    assertTrue("The entity was not expected to be returned", eLookup == null);

    q = new UsergridQuery.Builder()
        .collection(collectionName)
        .eq("orientation", "up")
        .build();

    eLookup = q.GET().first();

    assertTrue("The entity was not expected to be returned", eLookup == null);
  }
}
