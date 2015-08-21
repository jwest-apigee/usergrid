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
import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.exception.ClientException;
import org.apache.usergrid.java.client.response.ApiResponse;
import org.apache.usergrid.java.client.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.usergrid.java.client.utils.JsonUtils.*;
import static org.apache.usergrid.java.client.utils.MapUtils.newMapWithoutKeys;
import static org.apache.usergrid.java.client.utils.ObjectUtils.isEmpty;

public class UsergridEntity {

  private static final Logger log = LoggerFactory.getLogger(UsergridEntity.class);

  public static final String STR_UUID = "uuid";
  public final static String PROPERTY_UUID = STR_UUID;
  public final static String PROPERTY_TYPE = "type";
  public static final String STR_NAME = "name";

  public static Map<String, Class<? extends UsergridEntity>> CLASS_FOR_ENTITY_TYPE = new HashMap<String, Class<? extends UsergridEntity>>();

  static {
    CLASS_FOR_ENTITY_TYPE.put(User.ENTITY_TYPE, User.class);
  }

  protected Map<String, JsonNode> properties = new HashMap<String, JsonNode>();

  public UsergridEntity() {
  }

  public UsergridEntity(String type) {
    changeType(type);
  }

  public UsergridEntity(UsergridEntity fromCopy) {

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

  public void setUuid(UUID uuid) {
    setUUIDProperty(properties, PROPERTY_UUID, uuid);
  }

  @JsonAnyGetter
  public Map<String, JsonNode> getProperties() {
    return newMapWithoutKeys(properties, getPropertyNames());
  }

  @JsonAnySetter
  public void setProperty(String name, JsonNode value) {
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
  public void setProperty(String name, String value) {
    setStringProperty(properties, name, value);
  }

  /**
   * Set the property
   *
   * @param name
   * @param value
   */
  public void setProperty(String name, boolean value) {
    setBooleanProperty(properties, name, value);
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
  public void setProperty(String name, int value) {
    setProperty(name, (long) value);
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

  public static <T extends UsergridEntity> T toType(UsergridEntity usergridEntity, Class<T> t) {
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

  public static <T extends UsergridEntity> List<T> toType(List<UsergridEntity> entities,
                                                          Class<T> t) {
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

  public void save() throws ClientException {

    ApiResponse response = Usergrid.getInstance().save(this);
    //todo error checking on response : Done
    if ((response != null) && !isEmpty(response.getError())) {
      log.error("Client.apiRequest(): Response error: "
              + response.getError());
      if (!isEmpty(response.getException())) {
        log.error("Client.apiRequest(): Response exception: "
                + response.getException());
      }
    }

    UsergridEntity first = response.getFirstEntity();

    String uuid = first.getStringProperty(STR_UUID);
    this.setUuid(UUID.fromString(uuid));
  }

  public void delete() throws ClientException {
    // check for one of: name, uuid, error if not found
    if ( this.getUuid() == null && this.getStringProperty("name") == null)
      throw new IllegalArgumentException("No name or uuid is present for the entity. Invalid argument");

    ApiResponse response = Usergrid.getInstance().delete(this);
    //todo error checking on response : Done
    if ((response != null) && !isEmpty(response.getError())) {
      log.error("Client.apiRequest(): Response error: "
              + response.getError());
      if (!isEmpty(response.getException())) {
        log.error("Client.apiRequest(): Response exception: "
                + response.getException());
      }
    }

  }

  public String getStringProperty(String name) {
    return JsonUtils.getStringProperty(this.properties, name);
  }

  public <T> T getEntityProperty(String name) {
    return JsonUtils.getProperty(this.properties, name);
  }


  public void post() throws ClientException {

    ApiResponse response = Usergrid.getInstance().post(this);

    //todo error checking on response : Done
    if ((response != null) && !isEmpty(response.getError())) {
      log.error("Client.apiRequest(): Response error: "
              + response.getError());
      if (!isEmpty(response.getException())) {
        log.error("Client.apiRequest(): Response exception: "
                + response.getException());
      }
    }

    System.out.println(response);
    UsergridEntity first = response.getFirstEntity();
    String uuid = first.getStringProperty(STR_UUID);
    this.setUuid(UUID.fromString(uuid));
  }

  public void put() throws ClientException {

    // check for one of: name, uuid, error if not found
    if ( this.getUuid() == null && this.getStringProperty("name") == null)
       throw new IllegalArgumentException("No name or uuid is present for the entity. Invalid argument");

    ApiResponse response = Usergrid.getInstance().put(this);
    //todo error checking on response : Done
    if ((response != null) && !isEmpty(response.getError())) {
      log.error("Client.apiRequest(): Response error: "
              + response.getError());
      if (!isEmpty(response.getException())) {
        log.error("Client.apiRequest(): Response exception: "
                + response.getException());
      }
    }

    System.out.println(response);
    String uuid = response.getFirstEntity().getStringProperty(STR_UUID);

    // make sure there is an entity and a uuid
    this.setUuid(UUID.fromString(uuid));
  }

  public Connection connect(UsergridEntity target, String connectionType) throws ClientException {

    // check for one of: name, uuid, error if not found

    ApiResponse response = Usergrid.getInstance().connectEntities(
        this.getType(),
        this.getUuid() != null ? this.getUuid().toString() : this.getStringProperty(STR_NAME),
        connectionType,
        target.getUuid() != null ? target.getUuid().toString() : target.getStringProperty(STR_NAME));

    //todo - check to make sure it worked
    return new Connection(this, connectionType, target);
  }

  public static UsergridEntity copyOf(UsergridEntity fromEntity) {
    return null;
  }
}