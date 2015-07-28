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

import org.apache.usergrid.java.client.cache.CacheRequestInterceptor;
import org.apache.usergrid.java.client.model.*;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.response.ApiResponse;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.usergrid.java.client.utils.ObjectUtils.isEmpty;
import static org.springframework.util.StringUtils.arrayToDelimitedString;
import static org.springframework.util.StringUtils.tokenizeToStringArray;

/**
 * The Client class for accessing the Usergrid API. Start by instantiating this
 * class though the appropriate constructor.
 */
public class UsergridApache {

  private static final Map<String, UsergridApache> instances_;
  public static final String HTTP_POST = "POST";
  public static final String HEADER_AUTHORIZATION = "Authorization";
  public static final String BEARER = "Bearer ";
  public static final String HTTP_PUT = "PUT";
  public static final String HTTP_GET = "GET";
  public static final String HTTP_DELETE = "DELETE";
  public static final String STR_GROUPS = "groups";
  public static final String STR_USERS = "users";

  public static final String STR_DEFAULT = "default";
  public static final String STR_BLANK = "";

  static {

    instances_ = new HashMap<>(5);
    instances_.put(STR_DEFAULT, new UsergridApache());
  }

  public static UsergridApache getInstance() {

    return getInstance(STR_DEFAULT);
  }

  public static UsergridApache getInstance(String id) {

    UsergridApache client = instances_.get(id);

    if (client == null) {
      client = new UsergridApache();
      instances_.put(id, client);
    }

    return client;
  }

  private static final Logger log = LoggerFactory.getLogger(UsergridApache.class);

  public static boolean FORCE_PUBLIC_API = false;

  // Public API
  public static String PUBLIC_API_URL = "http://localhost:8080";

  // Local API of standalone server
  public static String LOCAL_STANDALONE_API_URL = "http://localhost:8080";

  private String apiUrl = PUBLIC_API_URL;
  private String organizationId;
  private String applicationId;
  private String clientId;
  private String clientSecret;
  private User loggedInUser = null;
  private String accessToken = null;
  private String currentOrganization = null;
  private javax.ws.rs.client.Client restClient;

  /**
   * Default constructor for instantiating a client.
   */
  public UsergridApache() {
    init();
  }

  /**
   * Instantiate client for a specific app
   *
   * @param applicationId the application id or name
   */
  public UsergridApache(final String organizationId,
                        final String applicationId) {
    init();
    this.organizationId = organizationId;
    this.applicationId = applicationId;
  }

  public void init() {

    restClient = ClientBuilder.newBuilder()
        .register(JacksonFeature.class)
        .register(CacheRequestInterceptor.class)
        .build();
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
  public void setApiUrl(final String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * @param apiUrl the Usergrid API url (default: http://api.usergrid.com)
   * @return Client object for method call chaining
   */
  public UsergridApache withApiUrl(final String apiUrl) {
    this.apiUrl = apiUrl;
    return this;
  }


  /**
   * the organizationId to set
   *
   * @param organizationId
   * @return
   */
  public UsergridApache withOrganizationId(final String organizationId) {
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
  public void setOrganizationId(final String organizationId) {
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
  public void setApplicationId(final String applicationId) {
    this.applicationId = applicationId;
  }


  /**
   * @param applicationId the application id or name
   * @return Client object for method call chaining
   */
  public UsergridApache withApplicationId(final String applicationId) {
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
  public void setClientId(final String clientId) {
    this.clientId = clientId;
  }

  /**
   * @param clientId the client key id for making calls as the application-owner.
   *                 Not safe for most mobile use.
   * @return Client object for method call chaining
   */
  public UsergridApache withClientId(final String clientId) {
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
  public void setClientSecret(final String clientSecret) {
    this.clientSecret = clientSecret;
  }

  /**
   * @param clientSecret the client key id for making calls as the application-owner.
   *                     Not safe for most mobile use.
   * @return Client object for method call chaining
   */
  public UsergridApache withClientSecret(final String clientSecret) {
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
  public void setLoggedInUser(final User loggedInUser) {
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
  public void setAccessToken(final String accessToken) {
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
  public void setCurrentOrganization(final String currentOrganization) {
    this.currentOrganization = currentOrganization;
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
  public ApiResponse apiRequest(final String method,
                                final Map<String, Object> params,
                                final Object data,
                                final String... segments) {

    // consider using Rx in the future: https://jersey.java.net/documentation/latest/rx-client.html

    // https://jersey.java.net/documentation/latest/client.html

    // required to appropriately set content-length when there is no content.  blank string results in '0'
    //   whereas null results in no header

    // default to JSON
    String contentType = MediaType.APPLICATION_JSON;

    Entity entity = Entity.entity(data == null ? STR_BLANK : data, contentType);

    // create the target from the base API URL
    WebTarget webTarget = restClient.target(apiUrl);

    // build the resource path from the segments
    for (String segment : segments)
      webTarget = webTarget.path(segment);

    // check to see if we need to do a FORM POST by checking the METHOD,
    // that there is NO DATA and that the params are not empty
    if (method.equals(HTTP_POST)
        && isEmpty(data)
        && !isEmpty(params)) {

      Form form = new Form();

      // build the FORM object
      for (Map.Entry<String, Object> param : params.entrySet()) {
        form.param(param.getKey(), String.valueOf(param.getValue()));
      }

      // set the content type
      contentType = MediaType.APPLICATION_FORM_URLENCODED;

      // update the entity being used to the FORM
      entity = Entity.form(form);

    } else {

      // this indicates this is not a POST

      // if there are params, then these are query params
      if (params != null) {

        // add the params to the URI
        for (Map.Entry<String, Object> entry : params.entrySet()) {
          webTarget = webTarget.queryParam(entry.getKey(), String.valueOf(entry.getValue()));
        }
      }
    }

    Invocation.Builder invocationBuilder = webTarget.request(contentType);

    // add access token
    // need to evaluate other authentication options here as well
    if (accessToken != null) {
      String auth = BEARER + accessToken;
      invocationBuilder.header(HEADER_AUTHORIZATION, auth);
    }

    // invoke the API Call
    return invocationBuilder.method(method, entity, ApiResponse.class);
  }


  public void assertInitialized() {

    if (isEmpty(applicationId)) {
      throw new IllegalArgumentException("No application id specified");
    }

    if (isEmpty(organizationId)) {
      throw new IllegalArgumentException("No organization id specified");
    }
  }

  /**
   * Log the user in and get a valid access token.
   *
   * @param email
   * @param password
   * @return non-null ApiResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public ApiResponse authorizeAppUser(final String email,
                                      final String password) {
    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(password, "password");
    assertInitialized();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<String, Object>();
    formData.put("grant_type", "password");
    formData.put("username", email);
    formData.put("password", password);
    ApiResponse response = apiRequest(HTTP_POST, formData, null,
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
  public ApiResponse changePassword(final String username,
                                    final String oldPassword,
                                    final String newPassword) {

    Map<String, Object> data = new HashMap<String, Object>();
    data.put("newpassword", newPassword);
    data.put("oldpassword", oldPassword);

    return apiRequest(HTTP_POST, null, data, organizationId, applicationId, STR_USERS,
        username, "password");

  }

  /**
   * Log the user in with their numeric pin-code and get a valid access token.
   *
   * @param email
   * @param pin
   * @return non-null ApiResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public ApiResponse authorizeAppUserViaPin(final String email,
                                            final String pin) {
    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(pin, "pin");
    assertInitialized();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<>();
    formData.put("grant_type", "pin");
    formData.put("username", email);
    formData.put("pin", pin);
    ApiResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "token");

    if (response == null) {
      return null;
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
   * @param fb_access_token
   * @return non-null ApiResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public ApiResponse authorizeAppUserViaFacebook(final String fb_access_token) {

    validateNonEmptyParam(fb_access_token, "Facebook token");
    assertInitialized();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<>();
    formData.put("fb_access_token", fb_access_token);
    ApiResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "auth", "facebook");

    if (response == null) {
      return null;
    }

    if (!isEmpty(response.getAccessToken()) && (response.getUser() != null)) {

      loggedInUser = response.getUser();
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppUserViaFacebook(): Access token: " + accessToken);

    } else {

      log.info("Client.authorizeAppUserViaFacebook(): Response: " + response);
    }

    return response;
  }

  /**
   * Log the app in with it's client id and client secret key. Not recommended
   * for production apps.
   *
   * @param clientId
   * @param clientSecret
   * @return non-null ApiResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public ApiResponse authorizeAppClient(final String clientId,
                                        final String clientSecret) {

    validateNonEmptyParam(clientId, "client identifier");
    validateNonEmptyParam(clientSecret, "client secret");
    assertInitialized();
    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<>();
    formData.put("grant_type", "client_credentials");
    formData.put("client_id", clientId);
    formData.put("client_secret", clientSecret);
    ApiResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "token");

    if (response == null) {
      return null;
    }

    if (!isEmpty(response.getAccessToken())) {
      loggedInUser = null;
      accessToken = response.getAccessToken();
      currentOrganization = null;
      log.info("Client.authorizeAppClient(): Access token: " + accessToken);

    } else {

      log.info("Client.authorizeAppClient(): Response: " + response);
    }

    return response;
  }

  private void validateNonEmptyParam(final Object param,
                                     final String paramName) {
    if (isEmpty(param)) {
      throw new IllegalArgumentException(paramName + " cannot be null or empty");
    }
  }

  /**
   * Registers a device using the device's unique device ID.
   *
   * @param deviceId
   * @param properties
   * @return a Device object if success
   */
  public Device registerDevice(final UUID deviceId,
                               Map<String, Object> properties) {
    assertInitialized();

    if (properties == null) {
      properties = new HashMap<>();
    }

    properties.put("refreshed", System.currentTimeMillis());
    ApiResponse response = apiRequest(HTTP_PUT, null, properties, organizationId, applicationId, "devices", deviceId.toString());

    return response.getFirstEntity(Device.class);
  }

  /**
   * Registers a device using the device's unique device ID.
   *
   * @param properties
   * @return a Device object if success
   */
  public Device registerDeviceForPush(final UUID deviceId,
                                      final String notifier,
                                      final String token,
                                      Map<String, Object> properties) {
    if (properties == null) {
      properties = new HashMap<>();
    }

    String notifierKey = notifier + ".notifier.id";
    properties.put(notifierKey, token);

    return registerDevice(deviceId, properties);
  }

  /**
   * Create a new usergridEntity on the server.
   *
   * @param usergridEntity
   * @return an ApiResponse with the new usergridEntity in it.
   */
  public ApiResponse createEntity(final UsergridEntity usergridEntity) {
    assertInitialized();

    if (isEmpty(usergridEntity.getType())) {
      throw new IllegalArgumentException("Missing usergridEntity type");
    }

    return apiRequest(HTTP_POST, null, usergridEntity, organizationId, applicationId, usergridEntity.getType());
  }


  /**
   * Create a new e on the server.
   *
   * @param e
   * @return an ApiResponse with the new e in it.
   */
  public ApiResponse updateEntity(final UsergridEntity e) {

    if (isEmpty(e.getType())) {
      throw new IllegalArgumentException("UsergridEntity is required to have a 'type' property and does not");
    }

    assertInitialized();

    String name = e.getStringProperty("name");
    String uuid = e.getStringProperty("uuid");

    if (name == null && uuid == null)
      return this.createEntity(e);

    String entityIdentifier = (uuid != null ? uuid : name);

    return apiRequest(HTTP_PUT, null, e.getProperties(), organizationId, applicationId, e.getType(), entityIdentifier);
  }

  /**
   * Create a new e on the server.
   *
   * @param e
   * @return an ApiResponse with the new e in it.
   */
  public ApiResponse save(final UsergridEntity e) {

    if (isEmpty(e.getType())) {
      throw new IllegalArgumentException("UsergridEntity is required to have a 'type' property and does not");
    }

    assertInitialized();

    String name = e.getStringProperty("name");
    String uuid = e.getStringProperty("uuid");

    String entityIdentifier = (uuid != null ? uuid : name);

    if (entityIdentifier == null) {
      return this.post(e);
    }

    return this.put(e);
  }


  /**
   * Create a new entity on the server from a set of properties. Properties
   * must include a "type" property.
   *
   * @param properties
   * @return an ApiResponse with the new entity in it.
   */
  public ApiResponse createEntity(Map<String, Object> properties) {

    assertInitialized();

    if (isEmpty(properties.get("type"))) {
      throw new IllegalArgumentException("Missing entity type");
    }

    return apiRequest(HTTP_POST, null, properties, organizationId, applicationId, properties.get("type").toString());
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
  public ApiResponse createUser(final String username,
                                final String name,
                                final String email,
                                final String password) {

    Map<String, Object> properties = new HashMap<>();
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
  public Map<String, Group> getGroupsForUser(final String userId) {

    ApiResponse response = apiRequest(HTTP_GET, null, null, organizationId, applicationId, STR_USERS, userId, STR_GROUPS);

    Map<String, Group> groupMap = new HashMap<>();

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
  public QueryResult queryActivityFeedForUser(final String userId) {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_USERS, userId, "feed");
  }

  /**
   * Posts an activity to a user. Activity must already be created.
   *
   * @param userId
   * @param activity
   * @return
   */
  public ApiResponse postUserActivity(final String userId, final Activity activity) {

    return apiRequest(HTTP_POST, null, activity, organizationId, applicationId, STR_USERS, userId, "activities");
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
  public ApiResponse postUserActivity(final String verb,
                                      final String title,
                                      final String content,
                                      final String category,
                                      final User user,
                                      final UsergridEntity object,
                                      final String objectType,
                                      final String objectName,
                                      final String objectContent) {

    Activity activity = Activity.newActivity(verb, title, content, category, user, object, objectType, objectName, objectContent);

    return postUserActivity(user.getUuid().toString(), activity);
  }

  /**
   * Posts an activity to a group. Activity must already be created.
   *
   * @param groupId
   * @param activity
   * @return
   */
  public ApiResponse postGroupActivity(final String groupId,
                                       final Activity activity) {

    return apiRequest(HTTP_POST, null, activity, organizationId, applicationId, STR_GROUPS, groupId, "activities");
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
  public ApiResponse postGroupActivity(final String groupId,
                                       final String verb,
                                       final String title,
                                       final String content,
                                       final String category,
                                       final User user,
                                       final UsergridEntity object,
                                       final String objectType,
                                       final String objectName,
                                       final String objectContent) {

    return postGroupActivity(groupId, Activity.newActivity(verb, title, content, category, user, object, objectType, objectName, objectContent));
  }

  /**
   * Post an activity to the stream.
   *
   * @param activity
   * @return
   */
  public ApiResponse postActivity(final Activity activity) {
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
  public ApiResponse postActivity(final String verb,
                                  final String title,
                                  final String content,
                                  final String category,
                                  final User user,
                                  final UsergridEntity object,
                                  final String objectType,
                                  final String objectName,
                                  final String objectContent) {

    return createEntity(Activity.newActivity(verb, title, content, category, user, object, objectType, objectName, objectContent));
  }

  /**
   * Get a group's activity feed. Returned as a query to ease paging.
   *
   * @return
   */
  public QueryResult queryActivity() {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, "activities");
  }


  /**
   * Get a group's activity feed. Returned as a query to ease paging.
   *
   * @param groupId
   * @return
   */
  public QueryResult queryActivityFeedForGroup(final String groupId) {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_GROUPS, groupId, "feed");
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
  public QueryResult queryEntities(final String method,
                                   final Map<String, Object> params,
                                   final Object data,
                                   final String... segments) {

    return new EntityQueryResult(apiRequest(method, params, data, segments), method, params, data, segments);
  }

  /**
   * Perform a query of the users collection.
   *
   * @return
   */
  public QueryResult queryUsers() {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_USERS);
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public QueryResult queryUsers(String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", ql);

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, STR_USERS);
  }

  /**
   * Perform a query of the users collection within the specified distance of
   * the specified location and optionally using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @return
   */
  public QueryResult queryUsersWithinLocation(final float distance,
                                              final float lattitude,
                                              final float longitude,
                                              final String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", this.makeLocationQL(distance, lattitude, longitude, ql));

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, STR_USERS);
  }

  public ApiResponse getEntity(final String type,
                               final String id) {

    return apiRequest(HTTP_GET, null, null, organizationId, applicationId, type, id);
  }

  public ApiResponse deleteEntity(final String type,
                                  final String id) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, type, id);
  }

  /**
   * Queries the users for the specified group.
   *
   * @param groupId
   * @return
   */
  public QueryResult queryUsersForGroup(final String groupId) {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_GROUPS, groupId, STR_USERS);
  }

  /**
   * Adds a user to the specified groups.
   *
   * @param userId
   * @param groupId
   * @return
   */
  public ApiResponse addUserToGroup(final String userId,
                                    final String groupId) {


    return apiRequest(HTTP_POST, null, null, organizationId, applicationId, STR_GROUPS, groupId, STR_USERS, userId);
  }

  /**
   * Creates a group with the specified group path. Group paths can be slash
   * ("/") delimited like file paths for hierarchical group relationships.
   *
   * @param groupPath
   * @return
   */
  public ApiResponse createGroup(final String groupPath) {
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
  public ApiResponse createGroup(final String groupPath,
                                 final String groupTitle) {

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
  public ApiResponse createGroup(final String groupPath,
                                 final String groupTitle,
                                 final String groupName) {

    Map<String, Object> data = new HashMap<>();
    data.put("type", "group");
    data.put("path", groupPath);

    if (groupTitle != null) {
      data.put("title", groupTitle);
    }

    if (groupName != null) {
      data.put("name", groupName);
    }

    return apiRequest(HTTP_POST, null, data, organizationId, applicationId, STR_GROUPS);
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public QueryResult queryGroups(final String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", ql);

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, STR_GROUPS);
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
  public ApiResponse connectEntities(final String connectingEntityType,
                                     final String connectingEntityId,
                                     final String connectionType,
                                     final String connectedEntityId) {

    return apiRequest(HTTP_POST, null, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType, connectedEntityId);
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
  public ApiResponse disconnectEntities(final String connectingEntityType,
                                        final String connectingEntityId,
                                        final String connectionType,
                                        final String connectedEntityId) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType, connectedEntityId);
  }


  /**
   * @param sourceVertex
   * @param TargetVertex
   * @param connetionName
   * @return
   */
  public ApiResponse disconnectEntities(final UsergridEntity sourceVertex,
                                        final UsergridEntity TargetVertex,
                                        final String connetionName) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName,
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
  public QueryResult queryEntityConnections(final String connectingEntityType,
                                            final String connectingEntityId,
                                            final String connectionType, String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", ql);

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType);
  }

  protected String makeLocationQL(float distance, double lattitude,
                                  double longitude, String ql) {
    String within = String.format("within %d of %d , %d", distance, lattitude, longitude);
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
  public QueryResult queryEntityConnectionsWithinLocation(final String connectingEntityType,
                                                          final String connectingEntityId,
                                                          final String connectionType,
                                                          final float distance,
                                                          float latitude,
                                                          final float longitude,
                                                          final String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", makeLocationQL(distance, latitude, longitude, ql));

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType);
  }


  public ApiResponse connectEntities(final UsergridEntity sourceVertex,
                                     final UsergridEntity TargetVertex,
                                     final String connetionName) {

    return this.connectEntities(sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName, TargetVertex.getUuid().toString());
  }


  public ApiResponse queryEdgesForVertex(final String srcType,
                                         final String srcID) {

    return apiRequest(HTTP_GET, null, null, organizationId, applicationId, srcType, srcID);
  }


  public ApiResponse queryCollections() {

    return apiRequest(HTTP_GET, null, null, this.organizationId, this.applicationId);
  }

  public ApiResponse queryConnection(final String... segments) {

    String[] paramPath = new String[10];
    paramPath[0] = this.organizationId;
    paramPath[1] = this.applicationId;
    System.arraycopy(segments, 0, paramPath, 2, segments.length);

    return apiRequest(HTTP_GET, null, null, paramPath);
  }

  public UsergridEntity getEntity(final String s) {
    return null;
  }

  private String convertStringArrayToPath(final String[] segments) {
    return null;
  }


  public QueryResult query(final UsergridQuery usergridQuery) {

    String uri = usergridQuery.toString();

    return null;
  }

  public ApiResponse put(final UsergridEntity usergridEntity) {
    return updateEntity(usergridEntity);
  }

  public ApiResponse post(final UsergridEntity usergridEntity) {
    return this.createEntity(usergridEntity);
  }

  public ApiResponse delete(final UsergridEntity usergridEntity) {
    return this.deleteEntity(usergridEntity.getType(), usergridEntity.getUuid().toString());
  }

  public static void initialize(String apiUrl, String orgName, String appName) {

    UsergridApache client = getInstance(STR_DEFAULT);
    client.withApiUrl(apiUrl)
        .withOrganizationId(orgName)
        .withApplicationId(appName);
  }

  public interface QueryResult {

    public ApiResponse getLastResponse();

    public boolean more();

    public QueryResult next();

  }

  /**
   * QueryResult object
   */
  private class EntityQueryResult implements QueryResult {

    final String method;
    final Map<String, Object> params;
    final Object data;
    final String[] segments;
    final ApiResponse lastResponse;

    private EntityQueryResult(final ApiResponse lastResponse,
                              final String method,
                              final Map<String, Object> params,
                              final Object data,
                              final String[] segments) {

      this.lastResponse = lastResponse;
      this.method = method;
      this.params = params;
      this.data = data;
      this.segments = segments;
    }

    private EntityQueryResult(final ApiResponse lastResponse,
                              final EntityQueryResult q) {

      this.lastResponse = lastResponse;
      method = q.method;
      params = q.params;
      data = q.data;
      segments = q.segments;
    }

    /**
     * @return the api lastResponse of the last request
     */
    public ApiResponse getLastResponse() {
      return lastResponse;
    }

    /**
     * @return true if the server indicates more results are available
     */
    public boolean more() {

      return (lastResponse != null)
          && (lastResponse.getCursor() != null)
          && (lastResponse.getCursor().length() > 0);
    }

    /**
     * Performs a request for the next set of results
     *
     * @return query that contains results and where to get more from.
     */
    public QueryResult next() {

      if (more()) {

        Map<String, Object> nextParams = null;

        if (params != null) {

          nextParams = new HashMap<>(params);

        } else {

          nextParams = new HashMap<>();
        }

        nextParams.put("cursor", lastResponse.getCursor());

        ApiResponse nextResponse = apiRequest(method, nextParams, data, segments);

        return new EntityQueryResult(nextResponse, this);
      }

      return null;
    }

  }

  private String normalizeQueuePath(final String path) {

    return arrayToDelimitedString(tokenizeToStringArray(path, "/", true, true), "/");
  }

  public ApiResponse postMessage(final String path,
                                 final Map<String, Object> message) {

    return apiRequest(HTTP_POST, null, message, organizationId, applicationId, "queues", normalizeQueuePath(path));
  }

  public ApiResponse postMessage(final String path,
                                 final List<Map<String, Object>> messages) {

    return apiRequest(HTTP_POST, null, messages, organizationId, applicationId, "queues", normalizeQueuePath(path));
  }

  public enum QueuePosition {

    START("start"), END("end"), LAST("last"), CONSUMER("consumer");

    private final String shortName;

    QueuePosition(String shortName) {
      this.shortName = shortName;
    }

    static Map<String, QueuePosition> nameMap = new ConcurrentHashMap<>();

    static {

      for (QueuePosition op : EnumSet.allOf(QueuePosition.class)) {

        if (op.shortName != null) {
          nameMap.put(op.shortName, op);
        }
      }
    }

    public static QueuePosition find(final String s) {

      if (s == null) {
        return null;
      }

      return nameMap.get(s);
    }

    @Override
    public String toString() {
      return shortName;
    }
  }

  public ApiResponse getMessages(final String path,
                                 final String consumer,
                                 final UUID last,
                                 final Long time,
                                 final Integer prev,
                                 final Integer next,
                                 final Integer limit,
                                 final QueuePosition pos,
                                 final Boolean update,
                                 final Boolean sync) {

    Map<String, Object> params = new HashMap<>();

    if (consumer != null) {
      params.put("consumer", consumer);
    }
    if (last != null) {
      params.put("last", last);
    }
    if (time != null) {
      params.put("time", time);
    }
    if (prev != null) {
      params.put("prev", prev);
    }
    if (next != null) {
      params.put("next", next);
    }
    if (limit != null) {
      params.put("limit", limit);
    }
    if (pos != null) {
      params.put("pos", pos.toString());
    }
    if (update != null) {
      params.put("update", update);
    }
    if (sync != null) {
      params.put("synchronized", sync);
    }

    return apiRequest(HTTP_GET, params, null, organizationId, applicationId, "queues", normalizeQueuePath(path));
  }

  public ApiResponse addSubscriber(final String publisherQueue,
                                   final String subscriberQueue) {

    return apiRequest(HTTP_POST, null, null, organizationId, applicationId, "queues", normalizeQueuePath(publisherQueue), "subscribers", normalizeQueuePath(subscriberQueue));
  }

  public ApiResponse removeSubscriber(final String publisherQueue,
                                      final String subscriberQueue) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, "queues", normalizeQueuePath(publisherQueue), "subscribers", normalizeQueuePath(subscriberQueue));
  }

  private class QueueQueryResult implements QueryResult {
    final String method;
    final Map<String, Object> params;
    final Object data;
    final String queuePath;
    final ApiResponse response;

    private QueueQueryResult(final ApiResponse response,
                             final String method,
                             final Map<String, Object> params,
                             final Object data,
                             final String queuePath) {

      this.response = response;
      this.method = method;
      this.params = params;
      this.data = data;
      this.queuePath = normalizeQueuePath(queuePath);
    }

    private QueueQueryResult(final ApiResponse response,
                             final QueueQueryResult q) {

      this.response = response;
      method = q.method;
      params = q.params;
      data = q.data;
      queuePath = q.queuePath;
    }

    /**
     * @return the api lastResponse of the last request
     */
    public ApiResponse getLastResponse() {
      return response;
    }

    /**
     * @return true if the server indicates more results are available
     */
    public boolean more() {

      return (response != null)
          && (response.getCursor() != null)
          && (response.getCursor().length() > 0);
    }

    /**
     * Performs a request for the next set of results
     *
     * @return query that contains results and where to get more from.
     */
    public QueryResult next() {

      if (more()) {

        Map<String, Object> nextParams = null;

        if (params != null) {

          nextParams = new HashMap<>(params);

        } else {

          nextParams = new HashMap<>();
        }

        nextParams.put("start", response.getCursor());
        ApiResponse nextResponse = apiRequest(method, nextParams, data, queuePath);

        return new QueueQueryResult(nextResponse, this);
      }

      return null;
    }

  }

  public QueryResult queryQueuesRequest(final String method,
                                        final Map<String, Object> params,
                                        final Object data,
                                        final String queuePath) {

    ApiResponse response = apiRequest(method, params, data, queuePath);

    return new QueueQueryResult(response, method, params, data, queuePath);
  }

}
