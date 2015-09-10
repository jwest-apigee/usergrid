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
package org.apache.usergrid.java.client.response;

import static org.apache.usergrid.java.client.utils.JsonUtils.toJsonString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.model.Message;
import org.apache.usergrid.java.client.model.User;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;

public class UsergridResponse {

  private String accessToken;

  private String error;
  private String errorDescription;
  private String errorUri;
  private String exception;

  private String path;
  private String uri;
  private String status;
  private long timestamp;
  private UUID application;
  private List<UsergridEntity> entities;
  private UUID next;
  private String cursor;
  private String action;
  private List<Object> list;
  private Object data;
  private Map<String, UUID> applications;
  private Map<String, JsonNode> metadata;
  private Map<String, List<String>> params;
  private List<AggregateCounterSet> counters;
  private ClientCredentialsInfo credentials;

  private List<Message> messages;
  private List<QueueInfo> queues;
  private UUID last;
  private UUID queue;
  private UUID consumer;

  private User user;

  private final Map<String, JsonNode> properties = new HashMap<String, JsonNode>();

  public UsergridResponse() {
  }

  @JsonAnyGetter
  public Map<String, JsonNode> getProperties() {
    return properties;
  }

  @JsonAnySetter
  public void setProperty(String key, JsonNode value) {
    properties.put(key, value);
  }

  @JsonProperty("access_token")
  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getAccessToken() {
    return accessToken;
  }

  @JsonProperty("access_token")
  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  @JsonProperty("error_description")
  public String getErrorDescription() {
    return errorDescription;
  }

  @JsonProperty("error_description")
  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  @JsonProperty("error_uri")
  public String getErrorUri() {
    return errorUri;
  }

  @JsonProperty("error_uri")
  public void setErrorUri(String errorUri) {
    this.errorUri = errorUri;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public UUID getApplication() {
    return application;
  }

  public void setApplication(UUID application) {
    this.application = application;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public List<UsergridEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<UsergridEntity> entities) {
    this.entities = entities;
  }

  public int getEntityCount() {
    if (entities == null) {
      return 0;
    }
    return entities.size();
  }

  public UsergridEntity getFirstEntity() {
    if ((entities != null) && (entities.size() > 0)) {
      return entities.get(0);
    }
    return null;
  }

  public <T extends UsergridEntity> T getFirstEntity(Class<T> t) {
    return UsergridEntity.toType(getFirstEntity(), t);
  }

  public UsergridEntity getLastEntity() {
    if ((entities != null) && (entities.size() > 0)) {
      return entities.get(entities.size() - 1);
    }
    return null;
  }

  public <T extends UsergridEntity> T getLastEntity(Class<T> t) {
    return UsergridEntity.toType(getLastEntity(), t);
  }

  public <T extends UsergridEntity> List<T> getEntities(Class<T> t) {
    return UsergridEntity.toType(entities, t);
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public UUID getNext() {
    return next;
  }

  public void setNext(UUID next) {
    this.next = next;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getCursor() {
    return cursor;
  }

  public void setCursor(String cursor) {
    this.cursor = cursor;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public List<Object> getList() {
    return list;
  }

  public void setList(List<Object> list) {
    this.list = list;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public Map<String, UUID> getApplications() {
    return applications;
  }

  public void setApplications(Map<String, UUID> applications) {
    this.applications = applications;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public Map<String, JsonNode> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, JsonNode> metadata) {
    this.metadata = metadata;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public Map<String, List<String>> getParams() {
    return params;
  }

  public void setParams(Map<String, List<String>> params) {
    this.params = params;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public List<AggregateCounterSet> getCounters() {
    return counters;
  }

  public void setCounters(List<AggregateCounterSet> counters) {
    this.counters = counters;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public ClientCredentialsInfo getCredentials() {
    return credentials;
  }

  public void setCredentials(ClientCredentialsInfo credentials) {
    this.credentials = credentials;
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }

  @Override
  public String toString() {
    return toJsonString(this);
  }

  @JsonSerialize(include = Inclusion.NON_NULL)
  public UUID getLast() {
    return last;
  }

  /**
   * get the first entity in the 'entities' array in the response
   *
   * @return A UsergridEntity if the entities array has elements, null otherwise
   */
  public UsergridEntity first() {
    if (getEntities() != null && getEntities().size() > 0) {
      return getEntities().get(0);
    }

    return null;
  }

  public static UsergridResponse fromException(Exception ex) {
    ex.printStackTrace();

    UsergridResponse response = new UsergridResponse();
    response.setError(ex.getMessage());

    if (ex instanceof BadRequestException) {
      BadRequestException bre = (BadRequestException) ex;
      Response r = bre.getResponse();

      response.setStatus(String.valueOf(r.getStatus()));
    }

    return response;
  }
}
