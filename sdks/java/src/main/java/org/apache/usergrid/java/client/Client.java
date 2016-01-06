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
package org.apache.usergrid.java.client;

import static org.springframework.util.StringUtils.arrayToDelimitedString;
import static org.springframework.util.StringUtils.tokenizeToStringArray;
import static org.apache.usergrid.java.client.utils.JsonUtils.parse;
import static org.apache.usergrid.java.client.utils.ObjectUtils.isEmpty;
import static org.apache.usergrid.java.client.utils.UrlUtils.addQueryParams;
import static org.apache.usergrid.java.client.utils.UrlUtils.encodeParams;
import static org.apache.usergrid.java.client.utils.UrlUtils.path;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import javax.annotation.Nullable;
import org.apache.usergrid.java.client.model.*;
import org.apache.usergrid.java.client.query.EntityQueryResult;
import org.apache.usergrid.java.client.query.QueryResult;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.response.UsergridResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * The Client class for accessing the Usergrid API. Start by instantiating this
 * class though the appropriate constructor.
 */
public class Client {

  private static final Logger log = LoggerFactory.getLogger(Client.class);
  public static final String STRING_UUID = "uuid";

  public static boolean FORCE_PUBLIC_API = false;

  // Public API
  public static String PUBLIC_API_URL = "http://localhost:8080";

  // Local API of standalone server
  public static String LOCAL_STANDALONE_API_URL = "http://localhost:8080";

  // Local API of Tomcat server in Eclipse
  public static String LOCAL_TOMCAT_API_URL = "http://localhost:8080/ROOT";

  // Local API
  public static String LOCAL_API_URL = LOCAL_STANDALONE_API_URL;

  private String apiUrl = PUBLIC_API_URL;

  private String organizationId;
  private String applicationId;
  private String clientId;
  private String clientSecret;

  private User loggedInUser = null;

  private String accessToken = null;

  private String currentOrganization = null;

  static RestTemplate restTemplate = new RestTemplate();

  /**
   * Default constructor for instantiating a client.
   */
  public Client() {
    init();
  }

  /**
   * Instantiate client for a specific app
   *
   * @param applicationId the application id or name
   */
  public Client(String organizationId, String applicationId) {
    init();
    this.organizationId = organizationId;
    this.applicationId = applicationId;
  }

  public void init() {

  }

  /**
   * @return the Usergrid API url (default: http://api.usergrid.com)
   */
  public String getApiUrl() {
    return apiUrl;
  }

  /**
   * @param apiUrl the Usergrid API url (default: http://api.usergrid.com)
   */
  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * @param apiUrl the Usergrid API url (default: http://api.usergrid.com)
   * @return Client object for method call chaining
   */
  public Client withApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
    return this;
  }


  /**
   * the organizationId to set
   *
   * @param organizationId
   * @return
   */
  public Client withOrganizationId(String organizationId) {
    this.organizationId = organizationId;
    return this;
  }


  /**
   * @return the organizationId
   */
  public String getOrganizationId() {
    return organizationId;
  }

  /**
   * @param organizationId the organizationId to set
   */
  public void setOrganizationId(String organizationId) {
    this.organizationId = organizationId;
  }

  /**
   * @return the application id or name
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * @param applicationId the application id or name
   */
  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }


  /**
   * @param applicationId the application id or name
   * @return Client object for method call chaining
   */
  public Client withApplicationId(String applicationId) {
    this.applicationId = applicationId;
    return this;
  }

  /**
   * @return the client key id for making calls as the application-owner. Not
   * safe for most mobile use.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * @param clientId the client key id for making calls as the application-owner.
   *                 Not safe for most mobile use.
   */
  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  /**
   * @param clientId the client key id for making calls as the application-owner.
   *                 Not safe for most mobile use.
   * @return Client object for method call chaining
   */
  public Client withClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  /**
   * @return the client key id for making calls as the application-owner. Not
   * safe for most mobile use.
   */
  public String getClientSecret() {
    return clientSecret;
  }

  /**
   * @param clientSecret the client key id for making calls as the application-owner.
   *                     Not safe for most mobile use.
   */
  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  /**
   * @param clientSecret the client key id for making calls as the application-owner.
   *                     Not safe for most mobile use.
   * @return Client object for method call chaining
   */
  public Client withClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
    return this;
  }

  /**
   * @return the logged-in user after a successful authorizeAppUser request
   */
  public User getLoggedInUser() {
    return loggedInUser;
  }

  /**
   * @param loggedInUser the logged-in user, usually not set by host application
   */
  public void setLoggedInUser(User loggedInUser) {
    this.loggedInUser = loggedInUser;
  }

  /**
   * @return the OAuth2 access token after a successful authorize request
   */
  public String getAccessToken() {
    return accessToken;
  }

  /**
   * @param accessToken an OAuth2 access token. Usually not set by host application
   */
  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  /**
   * @return the currentOrganization
   */
  public String getCurrentOrganization() {
    return currentOrganization;
  }

  /**
   * @param currentOrganization
   */
  public void setCurrentOrganization(String currentOrganization) {
    this.currentOrganization = currentOrganization;
  }

  /**
   * Low-level HTTP request method. Synchronous, blocks till response or
   * timeout.
   *
   * @param method   HttpMethod method
   * @param cls      class for the return type
   * @param params   parameters to encode as querystring or body parameters
   * @param data     JSON data to put in body
   * @param segments REST url path segments (i.e. /segment1/segment2/segment3)
   * @return results marshalled into class specified in cls parameter
   */
  public <T> T httpRequest(HttpMethod method, Class<T> cls,
                           Map<String, Object> params, Object data, String... segments) {

    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

    if (accessToken != null) {
      String auth = "Bearer " + accessToken;
      requestHeaders.set("Authorization", auth);
      log.info("Authorization: " + auth);
    }

    String url = path(apiUrl, segments);

    MediaType contentType = MediaType.APPLICATION_JSON;

    if (method.equals(HttpMethod.POST) && isEmpty(data) && !isEmpty(params)) {
      data = encodeParams(params);
      contentType = MediaType.APPLICATION_FORM_URLENCODED;
    } else {
      url = addQueryParams(url, params);
    }

    requestHeaders.setContentType(contentType);
    HttpEntity<?> requestEntity = null;

    if (method.equals(HttpMethod.POST) || method.equals(HttpMethod.PUT)) {

      if (isEmpty(data)) {
        data = JsonNodeFactory.instance.objectNode();
      }
      requestEntity = new HttpEntity<Object>(data, requestHeaders);
    } else {
      requestEntity = new HttpEntity<Object>(requestHeaders);
    }
    log.info("Client.httpRequest(): url: " + url);
    ResponseEntity<T> responseEntity = restTemplate.exchange(url, method,
        requestEntity, cls);
    log.info("Client.httpRequest(): reponse body: "
        + responseEntity.getBody().toString());
    return responseEntity.getBody();
  }

  /**
   * High-level Usergrid API request.
   *
   * @param method
   * @param params
   * @param data
   * @param segments
   * @return
   */
  public UsergridResponse apiRequest(HttpMethod method,
                                     Map<String, Object> params, Object data, String... segments) {
    UsergridResponse response = null;
    try {
      response = httpRequest(method, UsergridResponse.class, params, data,
          segments);
      log.info("Client.apiRequest(): Response: " + response);
    } catch (HttpClientErrorException e) {
      log.error("Client.apiRequest(): HTTP error: "
          + e.getLocalizedMessage());
      response = parse(e.getResponseBodyAsString(), UsergridResponse.class);
      if ((response != null) && !isEmpty(response.getError())) {
        log.error("Client.apiRequest(): Response error: "
            + response.getError());
        if (!isEmpty(response.getException())) {
          log.error("Client.apiRequest(): Response exception: "
              + response.getException());
        }
      }
    }
    return response;
  }


  public void assertValidApplicationId() {
    if (isEmpty(applicationId)) {
      throw new IllegalArgumentException("No application id specified");
    }
  }

  /**
   * Log the user in and get a valid access token.
   *
   * @param email
   * @param password
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUser(String email, String password) {
    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(password, "password");
    assertValidApplicationId();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<String, Object>();
    formData.put("grant_type", "password");
    formData.put("username", email);
    formData.put("password", password);
    UsergridResponse response = apiRequest(HttpMethod.POST, formData, null,
        organizationId, applicationId, "token");
    if (response == null) {
      return response;
    }
    if (!isEmpty(response.getAccessToken()) && (response.getUser() != null)) {
      loggedInUser = response.getUser();
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppUser(): Access token: " + accessToken);
    } else {
      log.info("Client.authorizeAppUser(): Response: " + response);
    }
    return response;
  }

  /**
   * Change the password for the currently logged in user. You must supply the
   * old password and the new password.
   *
   * @param username
   * @param oldPassword
   * @param newPassword
   * @return
   */
  public UsergridResponse changePassword(String username, String oldPassword,
                                         String newPassword) {

    Map<String, Object> data = new HashMap<String, Object>();
    data.put("newpassword", newPassword);
    data.put("oldpassword", oldPassword);

    return apiRequest(HttpMethod.POST, null, data, organizationId, applicationId, "users",
        username, "password");

  }

  /**
   * Log the user in with their numeric pin-code and get a valid access token.
   *
   * @param email
   * @param pin
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUserViaPin(String email, String pin) {
    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(pin, "pin");
    assertValidApplicationId();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<String, Object>();
    formData.put("grant_type", "pin");
    formData.put("username", email);
    formData.put("pin", pin);
    UsergridResponse response = apiRequest(HttpMethod.POST, formData, null,
        organizationId, applicationId, "token");
    if (response == null) {
      return response;
    }
    if (!isEmpty(response.getAccessToken()) && (response.getUser() != null)) {
      loggedInUser = response.getUser();
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppUser(): Access token: " + accessToken);
    } else {
      log.info("Client.authorizeAppUser(): Response: " + response);
    }
    return response;
  }

  /**
   * Log the user in with their Facebook access token retrived via Facebook
   * OAuth.
   *
   * @param email
   * @param pin
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUserViaFacebook(String fb_access_token) {
    validateNonEmptyParam(fb_access_token, "Facebook token");
    assertValidApplicationId();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<String, Object>();
    formData.put("fb_access_token", fb_access_token);
    UsergridResponse response = apiRequest(HttpMethod.POST, formData, null,
        organizationId, applicationId, "auth", "facebook");
    if (response == null) {
      return response;
    }
    if (!isEmpty(response.getAccessToken()) && (response.getUser() != null)) {
      loggedInUser = response.getUser();
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppUserViaFacebook(): Access token: "
          + accessToken);
    } else {
      log.info("Client.authorizeAppUserViaFacebook(): Response: "
          + response);
    }
    return response;
  }

  /**
   * Log the app in with it's client id and client secret key. Not recommended
   * for production apps.
   *
   * @param email
   * @param pin
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppClient(String clientId, String clientSecret) {
    validateNonEmptyParam(clientId, "client identifier");
    validateNonEmptyParam(clientSecret, "client secret");
    assertValidApplicationId();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<String, Object>();
    formData.put("grant_type", "client_credentials");
    formData.put("client_id", clientId);
    formData.put("client_secret", clientSecret);
    UsergridResponse response = apiRequest(HttpMethod.POST, formData, null,
        organizationId, applicationId, "token");
    if (response == null) {
      return response;
    }
    if (!isEmpty(response.getAccessToken())) {
      loggedInUser = null;
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppClient(): Access token: "
          + accessToken);
    } else {
      log.info("Client.authorizeAppClient(): Response: " + response);
    }
    return response;
  }

  private void validateNonEmptyParam(Object param, String paramName) {
    if (isEmpty(param)) {
      throw new IllegalArgumentException(paramName + " cannot be null or empty");
    }
  }

  /**
   * Registers a device using the device's unique device ID.
   *
   * @param context
   * @param properties
   * @return a Device object if success
   */
  public Device registerDevice(UUID deviceId, Map<String, Object> properties) {
    assertValidApplicationId();
    if (properties == null) {
      properties = new HashMap<String, Object>();
    }
    properties.put("refreshed", System.currentTimeMillis());
    UsergridResponse response = apiRequest(HttpMethod.PUT, null, properties,
        organizationId, applicationId, "devices", deviceId.toString());
    return response.getFirstEntity(Device.class);
  }

  /**
   * Registers a device using the device's unique device ID.
   *
   * @param context
   * @param properties
   * @return a Device object if success
   */
  public Device registerDeviceForPush(UUID deviceId,
                                      String notifier,
                                      String token,
                                      Map<String, Object> properties) {
    if (properties == null) {
      properties = new HashMap<String, Object>();
    }
    String notifierKey = notifier + ".notifier.id";
    properties.put(notifierKey, token);
    return registerDevice(deviceId, properties);
  }

  /**
   * Create a new usergridEntity on the server.
   *
   * @param usergridEntity
   * @return an UsergridResponse with the new usergridEntity in it.
   */
  public UsergridResponse createEntity(UsergridEntity usergridEntity) {
    assertValidApplicationId();
    if (isEmpty(usergridEntity.getType())) {
      throw new IllegalArgumentException("Missing usergridEntity type");
    }
    UsergridResponse response = apiRequest(HttpMethod.POST, null, usergridEntity,
        organizationId, applicationId, usergridEntity.getType());
    return response;
  }


  /**
   * Create a new e on the server.
   *
   * @param e
   * @return an UsergridResponse with the new e in it.
   */
  public UsergridResponse updateEntity(UsergridEntity e) {

    if (isEmpty(e.getType())) {
      throw new IllegalArgumentException("UsergridEntity is required to have a 'type' property and does not");
    }

    String name = e.getStringProperty("name");
    String uuid = e.getStringProperty(STRING_UUID);

    if (name == null && uuid == null)
      return this.createEntity(e);

    String entityIdentifier = (uuid != null ? uuid : name);

    assertValidApplicationId();

    return apiRequest(HttpMethod.PUT, null, e.getProperties(),
        organizationId, applicationId, e.getType(), entityIdentifier);
  }

  public UsergridResponse updateEntity(Map<String, Object> properties) {
    assertValidApplicationId();
    return apiRequest(HttpMethod.PUT, null, properties,
        organizationId, applicationId, properties.get("type").toString(), properties.get(STRING_UUID).toString());
  }

  /**
   * Create a new entity on the server from a set of properties. Properties
   * must include a "type" property.
   *
   * @param properties
   * @return an UsergridResponse with the new entity in it.
   */
  public UsergridResponse createEntity(Map<String, Object> properties) {
    assertValidApplicationId();
    if (isEmpty(properties.get("type"))) {
      throw new IllegalArgumentException("Missing entity type");
    }
    return apiRequest(HttpMethod.POST, null, properties,
        organizationId, applicationId, properties.get("type").toString());
  }


  /**
   * Creates a user.
   *
   * @param username required
   * @param name
   * @param email
   * @param password
   * @return
   */
  public UsergridResponse createUser(String username, String name, String email,
                                     String password) {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put("type", "user");
    if (username != null) {
      properties.put("username", username);
    }
    if (name != null) {
      properties.put("name", name);
    }
    if (email != null) {
      properties.put("email", email);
    }
    if (password != null) {
      properties.put("password", password);
    }
    return createEntity(properties);
  }

  /**
   * Get the groups for the user.
   *
   * @param userId
   * @return a map with the group path as the key and the Group entity as the
   * value
   */
  public Map<String, Group> getGroupsForUser(String userId) {
    UsergridResponse response = apiRequest(HttpMethod.GET, null, null,
        organizationId, applicationId, "users", userId, "groups");
    Map<String, Group> groupMap = new HashMap<String, Group>();
    if (response != null) {
      List<Group> groups = response.getEntities(Group.class);
      for (Group group : groups) {
        groupMap.put(group.getPath(), group);
      }
    }
    return groupMap;
  }

  /**
   * Get a user's activity feed. Returned as a query to ease paging.
   *
   * @param userId
   * @return
   */
  public QueryResult queryActivityFeedForUser(String userId) {
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, null, null,
        organizationId, applicationId, "users", userId, "feed");
    return q;
  }

  /**
   * Posts an activity to a user. Activity must already be created.
   *
   * @param userId
   * @param activity
   * @return
   */
  public UsergridResponse postUserActivity(String userId, Activity activity) {
    return apiRequest(HttpMethod.POST, null, activity, organizationId, applicationId, "users",
        userId, "activities");
  }

  /**
   * Creates and posts an activity to a user.
   *
   * @param verb
   * @param title
   * @param content
   * @param category
   * @param user
   * @param object
   * @param objectType
   * @param objectName
   * @param objectContent
   * @return
   */
  public UsergridResponse postUserActivity(String verb, String title,
                                           String content, String category, User user, UsergridEntity object,
                                           String objectType, String objectName, String objectContent) {
    Activity activity = Activity.newActivity(verb, title, content,
        category, user, object, objectType, objectName, objectContent);
    return postUserActivity(user.getUuid().toString(), activity);
  }

  /**
   * Posts an activity to a group. Activity must already be created.
   *
   * @param userId
   * @param activity
   * @return
   */
  public UsergridResponse postGroupActivity(String groupId, Activity activity) {
    return apiRequest(HttpMethod.POST, null, activity, organizationId, applicationId, "groups",
        groupId, "activities");
  }

  /**
   * Creates and posts an activity to a group.
   *
   * @param groupId
   * @param verb
   * @param title
   * @param content
   * @param category
   * @param user
   * @param object
   * @param objectType
   * @param objectName
   * @param objectContent
   * @return
   */
  public UsergridResponse postGroupActivity(String groupId, String verb, String title,
                                            String content, String category, User user, UsergridEntity object,
                                            String objectType, String objectName, String objectContent) {
    Activity activity = Activity.newActivity(verb, title, content,
        category, user, object, objectType, objectName, objectContent);
    return postGroupActivity(groupId, activity);
  }

  /**
   * Post an activity to the stream.
   *
   * @param activity
   * @return
   */
  public UsergridResponse postActivity(Activity activity) {
    return createEntity(activity);
  }

  /**
   * Creates and posts an activity to a group.
   *
   * @param verb
   * @param title
   * @param content
   * @param category
   * @param user
   * @param object
   * @param objectType
   * @param objectName
   * @param objectContent
   * @return
   */
  public UsergridResponse postActivity(String verb, String title,
                                       String content, String category, User user, UsergridEntity object,
                                       String objectType, String objectName, String objectContent) {
    Activity activity = Activity.newActivity(verb, title, content,
        category, user, object, objectType, objectName, objectContent);
    return createEntity(activity);
  }

  /**
   * Get a group's activity feed. Returned as a query to ease paging.
   *
   * @param userId
   * @return
   */
  public QueryResult queryActivity() {
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, null, null,
        organizationId, applicationId, "activities");
    return q;
  }


  /**
   * Get a group's activity feed. Returned as a query to ease paging.
   *
   * @param userId
   * @return
   */
  public QueryResult queryActivityFeedForGroup(String groupId) {
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, null, null, organizationId, applicationId, "groups", groupId, "feed");
    return q;
  }

  /**
   * Perform a query request and return a query object. The QueryResult object
   * provides a simple way of dealing with result sets that need to be
   * iterated or paged through.
   *
   * @param method
   * @param params
   * @param data
   * @param segments
   * @return
   */
  public QueryResult queryEntitiesRequest(HttpMethod method,
                                          Map<String, Object> params, Object data, String... segments) {
    UsergridResponse response = apiRequest(method, params, data, segments);
    return  null;
  }

  /**
   * Perform a query of the users collection.
   *
   * @return
   */
  public QueryResult queryUsers() {
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, null, null,
        organizationId, applicationId, "users");
    return q;
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public QueryResult queryUsers(String ql) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("ql", ql);
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, params, null, organizationId,
        applicationId, "users");
    return q;
  }

  /**
   * Perform a query of the users collection within the specified distance of
   * the specified location and optionally using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param distance
   * @param location
   * @param ql
   * @return
   */
  public QueryResult queryUsersWithinLocation(float distance, float lattitude,
                                              float longitude, String ql) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("ql",
        this.makeLocationQL(distance, lattitude, longitude, ql));
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, params, null, organizationId,
        applicationId, "users");
    return q;
  }

  public UsergridResponse queryEntity(String type, String id) {
    return apiRequest(HttpMethod.GET, null, null, organizationId, applicationId,
        type, id);
  }

  public UsergridResponse deleteEntity(String type, String id) {
    return apiRequest(HttpMethod.DELETE, null, null, organizationId, applicationId,
        type, id);
  }

  /**
   * Queries the users for the specified group.
   *
   * @param groupId
   * @return
   */
  public QueryResult queryUsersForGroup(String groupId) {
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, null, null, organizationId,
        applicationId, "groups", groupId, "users");
    return q;
  }

  /**
   * Adds a user to the specified groups.
   *
   * @param userId
   * @param groupId
   * @return
   */
  public UsergridResponse addUserToGroup(String userId, String groupId) {
    return apiRequest(HttpMethod.POST, null, null, organizationId, applicationId, "groups",
        groupId, "users", userId);
  }

  /**
   * Creates a group with the specified group path. Group paths can be slash
   * ("/") delimited like file paths for hierarchical group relationships.
   *
   * @param groupPath
   * @return
   */
  public UsergridResponse createGroup(String groupPath) {
    return createGroup(groupPath, null);
  }

  /**
   * Creates a group with the specified group path and group title. Group
   * paths can be slash ("/") delimited like file paths for hierarchical group
   * relationships.
   *
   * @param groupPath
   * @param groupTitle
   * @return
   */
  public UsergridResponse createGroup(String groupPath, String groupTitle) {
    return createGroup(groupPath, groupTitle, null);
  }

  /**
   * Create a group with a path, title and name
   *
   * @param groupPath
   * @param groupTitle
   * @param groupName
   * @return
   */
  public UsergridResponse createGroup(String groupPath, String groupTitle, String groupName) {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("type", "group");
    data.put("path", groupPath);

    if (groupTitle != null) {
      data.put("title", groupTitle);
    }

    if (groupName != null) {
      data.put("name", groupName);
    }

    return apiRequest(HttpMethod.POST, null, data, organizationId, applicationId, "groups");
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public QueryResult queryGroups(String ql) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("ql", ql);
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, params, null, organizationId,
        applicationId, "groups");
    return q;
  }


  /**
   * Connect two entities together.
   *
   * @param connectingEntityType
   * @param connectingEntityId
   * @param connectionType
   * @param connectedEntityId
   * @return
   */
  public UsergridResponse connectEntities(
      String connectingEntityType,
      String connectingEntityId,
      String connectionType,
      String connectedEntityId) {
    return apiRequest(HttpMethod.POST, null, null, organizationId, applicationId,
        connectingEntityType, connectingEntityId, connectionType,
        connectedEntityId);
  }

  /**
   * Disconnect two entities.
   *
   * @param connectingEntityType
   * @param connectingEntityId
   * @param connectionType
   * @param connectedEntityId
   * @return
   */
  public UsergridResponse disconnectEntities(String connectingEntityType,
                                             String connectingEntityId, String connectionType,
                                             String connectedEntityId) {
    return apiRequest(HttpMethod.DELETE, null, null, organizationId, applicationId,
        connectingEntityType, connectingEntityId, connectionType,
        connectedEntityId);
  }


  /**
   * @param sourceVertex
   * @param TargetVertex
   * @param connetionName
   * @return
   */
  public UsergridResponse disconnectEntities(UsergridEntity sourceVertex, UsergridEntity TargetVertex, String connetionName) {

    return apiRequest(HttpMethod.DELETE, null, null, organizationId, applicationId,
        sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName,
        TargetVertex.getUuid().toString());
  }


  /**
   * QueryResult the connected entities.
   *
   * @param connectingEntityType
   * @param connectingEntityId
   * @param connectionType
   * @param ql
   * @return
   */
  public QueryResult queryEntityConnections(String connectingEntityType,
                                            String connectingEntityId, String connectionType, String ql) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("ql", ql);
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, params, null,
        organizationId, applicationId, connectingEntityType, connectingEntityId,
        connectionType);
    return q;
  }

  protected String makeLocationQL(float distance, double lattitude,
                                  double longitude, String ql) {
    String within = String.format("within %d of %d , %d", distance,
        lattitude, longitude);
    ql = ql == null ? within : within + " and " + ql;
    return ql;
  }

  /**
   * QueryResult the connected entities within distance of a specific point.
   *
   * @param connectingEntityType
   * @param connectingEntityId
   * @param connectionType
   * @param distance
   * @param latitude
   * @param longitude
   * @return
   */
  public QueryResult queryEntityConnectionsWithinLocation(
      String connectingEntityType, String connectingEntityId,
      String connectionType, float distance, float lattitude,
      float longitude, String ql) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("ql", makeLocationQL(distance, lattitude, longitude, ql));
    QueryResult q = queryEntitiesRequest(HttpMethod.GET, params, null, organizationId,
        applicationId, connectingEntityType, connectingEntityId,
        connectionType);
    return q;
  }

  public static void save(final UsergridEntity usergridEntity) {

  }

  public UsergridResponse connectEntities(UsergridEntity sourceVertex, UsergridEntity TargetVertex, String connetionName) {

    return this.connectEntities(sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName, TargetVertex.getUuid().toString());
  }


  public UsergridResponse queryEdgesForVertex(String srcType, String srcID) {
    return apiRequest(HttpMethod.GET, null, null, organizationId, applicationId,
        srcType, srcID);
  }


  public UsergridResponse queryCollections() {
    return apiRequest(HttpMethod.GET, null, null, this.organizationId, this.applicationId);
  }

  public UsergridResponse queryConnection(String... segments) {
    String[] paramPath = new String[10];
    paramPath[0] = this.organizationId;
    paramPath[1] = this.applicationId;
    for (int i = 0; i < segments.length; i++) {
      paramPath[2 + i] = segments[i];
    }
    return apiRequest(HttpMethod.GET, null, null, paramPath);

  }

  @Nullable
  public UsergridEntity getEntity(String s) {
    return null;
  }

  @Nullable
  public QueryResult query(UsergridQuery usergridQuery) {

    String uri = usergridQuery.toString();

    return null;
  }

  public UsergridResponse put(UsergridEntity usergridEntity) {
    return updateEntity(usergridEntity);
  }

  public UsergridResponse post(UsergridEntity usergridEntity) {
    return this.createEntity(usergridEntity);
  }

  public UsergridResponse delete(UsergridEntity usergridEntity) {
    return this.deleteEntity(usergridEntity.getType(), usergridEntity.getUuid().toString());
  }
}