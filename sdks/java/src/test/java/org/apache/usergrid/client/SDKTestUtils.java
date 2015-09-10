package org.apache.usergrid.client;

import org.apache.usergrid.java.client.model.UsergridEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by ApigeeCorporation on 9/10/15.
 */
public class SDKTestUtils {


  public static Map<String, UsergridEntity> createColorShapes(String collection) {

    Map<String, Map<String, String>> entityMap = new HashMap<>(7);

    Map<String, String> fields = new HashMap<>(3);
    fields.put("color", "red");
    fields.put("shape", "square");

    entityMap.put("redsquare", fields);

    fields = new HashMap<>(3);
    fields.put("color", "blue");
    fields.put("shape", "circle");

    entityMap.put("bluecircle", fields);

    fields = new HashMap<>(3);
    fields.put("color", "yellow");
    fields.put("shape", "triangle");

    entityMap.put("yellowtriangle", fields);

    return createEntities(collection, entityMap);
  }

  public static  Map<String, UsergridEntity> createEntities(final String collection,
                                                    final Map<String, Map<String, String>> entities) {

    Map<String, UsergridEntity> entityMap = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> entity : entities.entrySet()) {

      UsergridEntity e = createEntity(collection, entity.getKey(), entity.getValue());
      entityMap.put(e.getUuidString(), e);
    }

    return entityMap;
  }

  public static  UsergridEntity createEntity(String collection, String name, Map<String, String> fields) {

    UsergridEntity e = new UsergridEntity(collection);
    e.setProperty("name", name);

    for (Map.Entry<String, String> field : fields.entrySet()) {
      e.setProperty(field.getKey(), field.getValue());
    }

    e.POST();

    assertTrue("UUID should not be null", e.getUuidString() != null);

    for (Map.Entry<String, String> field : fields.entrySet()) {
      assertTrue("attempted to set a property which did not persist on the entity", e.getStringProperty(field.getKey()).equals(field.getValue()));
    }

    return e;
  }

}
