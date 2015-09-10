/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.java.client.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.exception.ClientException;
import org.apache.usergrid.java.client.response.UsergridResponse;
import org.apache.usergrid.java.client.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.usergrid.java.client.utils.JsonUtils.*;
import static org.apache.usergrid.java.client.utils.MapUtils.newMapWithoutKeys;

public class UsergridEntity {

  private static final Logger log = LoggerFactory.getLogger(UsergridEntity.class);

  public static final String STR_UUID = "uuid";
  public final static String PROPERTY_UUID = STR_UUID;
  public final static String PROPERTY_TYPE = "type";
  public static final String STR_NAME = "name";

  public static Map<String, Class<? extends UsergridEntity>> CLASS_FOR_ENTITY_TYPE = new HashMap<>();

  static {
    CLASS_FOR_ENTITY_TYPE.put(User.ENTITY_TYPE, User.class);
  }

  protected Map<String, JsonNode> properties = new HashMap<>();

  public UsergridEntity() {
  }

  public UsergridEntity(final String type) {
    changeType(type);
  }

  public UsergridEntity(final UsergridEntity fromCopy) {

  }

  @JsonIgnore
  public String getNativeType() {
    return getType();
  }

  @JsonIgnore
  public List<String> getPropertyNames() {
    List<String> properties = new ArrayList<>();
    properties.add(PROPERTY_TYPE);
    properties.add(PROPERTY_UUID);
    return properties;
  }

  public void setType(String type) {
    setStringProperty(properties, PROPERTY_TYPE, type);
  }

  public String getType() {
    return JsonUtils.getStringProperty(properties, PROPERTY_TYPE);
  }

  public void changeType(String type) {
    // get original type
    // if different, delete old entity in old collection and create new in new collection
    setStringProperty(properties, PROPERTY_TYPE, type);
  }

  public UUID getUuid() {
    return getUUIDProperty(properties, PROPERTY_UUID);
  }

  public String getUuidString() {
    return getStringProperty(PROPERTY_UUID);
  }

  public void setUuid(UUID uuid) {
    setUUIDProperty(properties, PROPERTY_UUID, uuid);
  }

  @JsonAnyGetter
  public Map<String, JsonNode> getProperties() {
    return newMapWithoutKeys(properties, getPropertyNames());
  }

  @JsonAnySetter
  public void setProperty(final String name,
                          final JsonNode value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, value);
    }
  }

  /**
   * Set the property
   *
   * @param name
   * @param value
   */
  public UsergridEntity setProperty(String name, String value) {
    setStringProperty(properties, name, value);

    return this;
  }


  /**
   * Set the property
   *
   * @param name
   * @param value
   */
  public void setProperty(String name, long value) {
    setLongProperty(properties, name, value);
  }

  /**
   * Set the property
   *
   * @param name
   * @param value
   */
  public UsergridEntity setProperty(String name, int value) {
    setProperty(name, (long) value);

    return this;
  }

  /**
   * Set the property
   *
   * @param name
   * @param value
   */
  public void setProperty(String name, float value) {
    setFloatProperty(properties, name, value);
  }

  @Override
  public String toString() {
    return toJsonString(this);
  }

  public <T extends UsergridEntity> T toType(Class<T> t) {
    return toType(this, t);
  }

  public static <T extends UsergridEntity> T toType(final UsergridEntity usergridEntity,
                                                    final Class<T> t) {
    if (usergridEntity == null) {
      return null;
    }

    T newEntity = null;

    if (usergridEntity.getClass().isAssignableFrom(t)) {
      try {
        newEntity = (t.newInstance());
        if ((newEntity.getNativeType() != null)
            && newEntity.getNativeType().equals(usergridEntity.getType())) {
          newEntity.properties = usergridEntity.properties;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return newEntity;
  }

  public static <T extends UsergridEntity> List<T> toType(final List<UsergridEntity> entities,
                                                          final Class<T> t) {

    List<T> l = new ArrayList<T>(entities != null ? entities.size() : 0);

    if (entities != null) {
      for (UsergridEntity usergridEntity : entities) {
        T newEntity = usergridEntity.toType(t);
        if (newEntity != null) {
          l.add(newEntity);
        }
      }
    }

    return l;
  }

  /**
   * Performs a DELETE of this entity using the Singleton client
   *
   * @throws ClientException
   */
  public UsergridResponse DELETE() throws ClientException {

    return Usergrid.getInstance().DELETE(this);
  }

  public String getStringProperty(String name) {
    return JsonUtils.getStringProperty(this.properties, name);
  }

  public <T> T getEntityProperty(String name) {
    return JsonUtils.getProperty(this.properties, name);
  }


  /**
   * Performs a POST of the entity using the Singleton client
   *
   * @throws ClientException
   */
  public UsergridResponse POST() throws ClientException {

    UsergridResponse response = Usergrid.getInstance().POST(this);
    if (response != null) {
      UsergridEntity first = response.first();
      this.refresh(first);

      return response;
    }

    throw new ClientException("Response was null on POST!");
  }

  /**
   * Performs a PUT of this entity using the Singleton client
   *
   * @throws ClientException
   */
  public UsergridResponse PUT() throws ClientException {

    // check for one of: name, uuid, error if not found
    if (this.getUuid() == null && this.getStringProperty("name") == null)
      throw new IllegalArgumentException("No name or uuid is present for the entity. Invalid argument");

    UsergridResponse response = Usergrid.getInstance().PUT(this);

    this.refresh(response.first());

    return response;
  }

  /**
   * Will refresh this object with a response from the server.  For example, when you do a POST and get back the
   * entity after a POST and setting the UUID
   *
   * @param newEntity
   */
  private void refresh(final UsergridEntity newEntity) {
    if (newEntity == null)
      return;

    String uuid = newEntity.getStringProperty(STR_UUID);

    // make sure there is an entity and a uuid
    this.setUuid(UUID.fromString(uuid));

    //todo - what else?
  }

  public Connection createConnection(final UsergridEntity target,
                                     final String connectionType) throws ClientException {

    // check for one of: name, uuid, error if not found

    UsergridResponse response = this.connect(target, connectionType);
    //todo check to make sure it worked

    return new Connection(this, connectionType, target);
  }

  public UsergridResponse connect(final UsergridEntity target,
                                  final String connectionType) throws ClientException {

    if (target.getUuid() != null) {
      return Usergrid.getInstance().connectEntities(
          this.getType(),
          this.getUuid() != null ? this.getUuid().toString() : this.getName(),
          connectionType,
          target.getUuid().toString());

    } else if (target.getType() != null && target.getName() != null) {
      return Usergrid.getInstance().connectEntities(
          this.getType(),
          this.getUuid() != null ? this.getUuid().toString() : this.getName(),
          connectionType,
          target.getType(),
          target.getName());

    } else {
      throw new IllegalArgumentException("One of UUID or Type+Name is required for the target entity of the connection");
    }
  }

  public static UsergridEntity copyOf(final UsergridEntity fromEntity) {
    return null;
  }

  /**
   * Retrieves an Entity by type and name
   *
   * @param collectionName the name of the collection
   * @param name           the name or UUID of the object
   * @return
   */
  public static UsergridEntity GET(String collectionName, String name) {
    return Usergrid.getInstance().getEntity(collectionName, name).first();
  }

  /**
   * Will effectively delete a property when it is set to null.  The property will not be
   * removed from the entity on the server side until a PUT is made
   *
   * @param propertyName
   */
  public void deleteProperty(String propertyName) {

    setProperty(propertyName, "");
  }

  public String getName() {
    return getStringProperty(STR_NAME);
  }

  /**
   * Set the location of an entity
   *
   * @param latitude
   * @param longitude
   */
  public void setLocation(double latitude, double longitude) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode(); // will be of type ObjectNode
    rootNode.put("latitude", latitude);
    rootNode.put("longitude", longitude);

    setObjectProperty(properties, "location", rootNode);
  }
}