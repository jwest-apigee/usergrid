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

import org.apache.usergrid.java.client.model.*;
import org.apache.usergrid.java.client.query.EntityQueryResult;
import org.apache.usergrid.java.client.query.LegacyQueryResult;
import org.apache.usergrid.java.client.query.QueryResult;
import org.apache.usergrid.java.client.query.UsergridQuery;
import org.apache.usergrid.java.client.response.UsergridResponse;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static org.apache.usergrid.java.client.utils.ObjectUtils.isEmpty;

/**
 * The Client class for accessing the Usergrid API. Start by instantiating this
 * class though the appropriate constructor.
 */
public class UsergridClient {

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

  private static final Logger log = LoggerFactory.getLogger(UsergridClient.class);

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
  private javax.ws.rs.client.Client restClient;

  /**
   * Default constructor for instantiating a client.
   */
  public UsergridClient() {
    init();
  }

  /**
   * Instantiate client for a specific app
   *
   * @param applicationId the application id or name
   */
  public UsergridClient(final String organizationId,
                        final String applicationId) {
    init();
    this.organizationId = organizationId;
    this.applicationId = applicationId;
  }

  public void init() {

    restClient = ClientBuilder.newBuilder()
        .register(JacksonFeature.class)
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
  public UsergridClient withApiUrl(final String apiUrl) {
    this.apiUrl = apiUrl;
    return this;
  }


  /**
   * the organizationId to set
   *
   * @param organizationId
   * @return
   */
  public UsergridClient withOrganizationId(final String organizationId) {
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
  public UsergridClient withApplicationId(final String applicationId) {
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
  public UsergridClient withClientId(final String clientId) {
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
  public UsergridClient withClientSecret(final String clientSecret) {
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
   * @param method   the HTTP Method
   * @param params   a Map of query parameters
   * @param data     the object to use in the body
   * @param segments the segments/of/the/uri
   * @return a UsergridResponse object
   */
  public UsergridResponse apiRequestWithSegmentArray(final String method,
                                                     final Map<String, Object> params,
                                                     Object data,
                                                     final String... segments) {
    return null;
  }

  /**
   * High-level Usergrid API request.
   *
   * @param method   the HTTP Method
   * @param params   a Map of query parameters
   * @param data     the object to use in the body
   * @param segments the segments/of/the/uri
   * @return a UsergridResponse object
   */
  public UsergridResponse apiRequest(final String method,
                                     final Map<String, Object> params,
                                     Object data,
                                     final String... segments) {
    assertInitialized();

    // https://jersey.java.net/documentation/latest/client.html

    // default to JSON
    String contentType = MediaType.APPLICATION_JSON;

    Entity entity = Entity.entity(data == null ? STR_BLANK : data, contentType);

    // create the target from the base API URL
    WebTarget webTarget = restClient.target(apiUrl);

    for (String segment : segments)
      if (segment != null)
        webTarget = webTarget.path(segment);

    if ((method.equals(HTTP_GET) || method.equals(HTTP_PUT) || method.equals(HTTP_DELETE)) && !isEmpty(params)) {
      for (Map.Entry<String, Object> param : params.entrySet()) {
        webTarget = webTarget.queryParam(param.getKey(), param.getValue());
      }
    }

    System.out.println(webTarget);


    // check to see if we need to do a FORM POST by checking the METHOD,
    // that there is NO DATA and that the params are not empty

    Form form = new Form();
    if (method.equals(HTTP_POST)
        && isEmpty(data)
        && !isEmpty(params)) {

      //TODO: Uncomment once fixed
      //for (Map.Entry<String, Object> param : params.entrySet()) {
      //  form.param(param.getKey(), String.valueOf(param.getValue()));
      //}
      data = params;
    }

    Invocation.Builder invocationBuilder = webTarget.request(contentType);

    // todo: need to evaluate other authentication options here as well
    if (accessToken != null) {
      String auth = BEARER + accessToken;
      invocationBuilder.header(HEADER_AUTHORIZATION, auth);
    }

    try {
      if (Objects.equals(method, HTTP_POST) || Objects.equals(method, HTTP_PUT)) {

        return invocationBuilder.method(method,
            entity,
            UsergridResponse.class);

      } else {

        return invocationBuilder.method(method,
            null,
            UsergridResponse.class);
      }
    } catch (Exception badRequestException) {
      return UsergridResponse.fromException(badRequestException);
    }

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
   * @param email    the email address of the user to get an access token for
   * @param password the user's password
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUser(final String email,
                                           final String password) {

    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(password, "password");

    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<>();
    formData.put("grant_type", "password");
    formData.put("username", email);
    formData.put("password", password);

    UsergridResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "token");

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
   * Change the password for the currently logged in user. You must supply the
   * old password and the new password.
   *
   * @param username    the app username for whom to change the password
   * @param oldPassword the user's old password
   * @param newPassword the user's new password
   * @return UsergridResponse which encapsulates the API response
   */
  public UsergridResponse changePassword(final String username,
                                         final String oldPassword,
                                         final String newPassword) {

    Map<String, Object> data = new HashMap<>();
    data.put("newpassword", newPassword);
    data.put("oldpassword", oldPassword);

    return apiRequest(HTTP_POST, null, data, organizationId, applicationId, STR_USERS, username, "password");

  }

  /**
   * Log the user in with their numeric pin-code and get a valid access token.
   *
   * @param email
   * @param pin
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUserViaPin(final String email,
                                                 final String pin) {


    validateNonEmptyParam(email, "email");
    validateNonEmptyParam(pin, "pin");

    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;

    Map<String, Object> formData = new HashMap<>();
    formData.put("grant_type", "pin");
    formData.put("username", email);
    formData.put("pin", pin);

    UsergridResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "token");

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
   * @param fb_access_token the access token from Facebook
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppUserViaFacebook(final String fb_access_token) {

    validateNonEmptyParam(fb_access_token, "Facebook token");

    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> formData = new HashMap<>();
    formData.put("fb_access_token", fb_access_token);
    UsergridResponse response = apiRequest(HTTP_POST, formData, null, organizationId, applicationId, "auth", "facebook");

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
   * @param clientId     the clientId of the Application from the 'App Overview' page in Usergrid UI
   * @param clientSecret the clientSecret of the Application from the 'App Overview' page in Usergrid UI
   * @return non-null UsergridResponse if request succeeds, check getError() for
   * "invalid_grant" to see if access is denied.
   */
  public UsergridResponse authorizeAppClient(final String clientId,
                                             final String clientSecret) {

    validateNonEmptyParam(clientId, "client identifier");
    validateNonEmptyParam(clientSecret, "client secret");

    loggedInUser = null;
    accessToken = null;
    currentOrganization = null;
    Map<String, Object> data = new HashMap<>();
    data.put("grant_type", "client_credentials");
    data.put("client_id", clientId);
    data.put("client_secret", clientSecret);
    UsergridResponse response = apiRequest(HTTP_POST, null, data, organizationId, applicationId, "token");

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
   * @param deviceId   the ID of the device
   * @param properties the attributes of the device
   * @return a Device object if success
   */
  public Device registerDevice(final UUID deviceId,
                               Map<String, Object> properties) {


    if (properties == null) {
      properties = new HashMap<>();
    }

    properties.put("refreshed", System.currentTimeMillis());
    UsergridResponse response = apiRequest(HTTP_PUT, null, properties, organizationId, applicationId, "devices", deviceId.toString());

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
   * @return an UsergridResponse with the new usergridEntity in it.
   */
  public UsergridResponse createEntity(final UsergridEntity usergridEntity) {

    if (isEmpty(usergridEntity.getType())) {
      throw new IllegalArgumentException("Missing usergridEntity type");
    }

    return apiRequest(HTTP_POST, null, usergridEntity, organizationId, applicationId, usergridEntity.getType());
  }

  /**
   * Create a new entity on the server from a set of properties. Properties
   * must include a "type" property.
   *
   * @param properties
   * @return an UsergridResponse with the new entity in it.
   */
  public UsergridResponse createEntity(Map<String, Object> properties) {


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
  public UsergridResponse createUser(final String username,
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

    UsergridResponse response = apiRequest(HTTP_GET, null, null, organizationId, applicationId, STR_USERS, userId, STR_GROUPS);

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
  public LegacyQueryResult queryActivityFeedForUser(final String userId) {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_USERS, userId, "feed");
  }

  /**
   * Posts an activity to a user. Activity must already be created.
   *
   * @param userId
   * @param activity
   * @return
   */
  public UsergridResponse postUserActivity(final String userId, final Activity activity) {

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
  public UsergridResponse postUserActivity(final String verb,
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
  public UsergridResponse postGroupActivity(final String groupId,
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
  public UsergridResponse postGroupActivity(final String groupId,
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
  public UsergridResponse postActivity(final Activity activity) {
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
  public UsergridResponse postActivity(final String verb,
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
  public LegacyQueryResult queryActivity() {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, "activities");
  }


  /**
   * Get a group's activity feed. Returned as a query to ease paging.
   *
   * @param groupId
   * @return
   */
  public LegacyQueryResult queryActivityFeedForGroup(final String groupId) {

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
  public LegacyQueryResult queryEntities(final String method,
                                         final Map<String, Object> params,
                                         final Object data,
                                         final String... segments) {

    return new EntityQueryResult(this, apiRequest(method, params, data, segments), method, params, data, segments);
  }

  /**
   * Perform a query of the users collection.
   *
   * @return
   */
  public LegacyQueryResult queryUsers() {
    UsergridQuery q = new UsergridQuery.Builder()
        .collection("users")
        .desc("created")
        .build();

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_USERS);
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public LegacyQueryResult queryUsers(String ql) {

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
  public LegacyQueryResult queryUsersWithinLocation(final float distance,
                                                    final float lattitude,
                                                    final float longitude,
                                                    final String ql) {

    Map<String, Object> params = new HashMap<>();
    params.put("ql", this.makeLocationQL(distance, lattitude, longitude, ql));

    return queryEntities(HTTP_GET, params, null, organizationId, applicationId, STR_USERS);
  }

  public UsergridResponse getEntity(final String type, final String id) {

    return apiRequest(HTTP_GET, null, null, organizationId, applicationId, type, id);
  }

  public UsergridResponse deleteEntity(final String type,
                                       final String id) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, type, id);
  }

  /**
   * Queries the users for the specified group.
   *
   * @param groupId
   * @return
   */
  public LegacyQueryResult queryUsersForGroup(final String groupId) {

    return queryEntities(HTTP_GET, null, null, organizationId, applicationId, STR_GROUPS, groupId, STR_USERS);
  }

  /**
   * Adds a user to the specified groups.
   *
   * @param userId
   * @param groupId
   * @return
   */
  public UsergridResponse addUserToGroup(final String userId,
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
  public UsergridResponse createGroup(final String groupPath) {
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
  public UsergridResponse createGroup(final String groupPath,
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
  public UsergridResponse createGroup(final String groupPath,
                                      final String groupTitle,
                                      final String groupName) {

    UsergridEntity group = new UsergridEntity(groupName);
    group.setProperty("path", groupPath);


    if (groupTitle != null) {
      group.setProperty("title", groupTitle);
    }

    if (groupName != null) {
      group.setProperty("name", groupName);
    }

    return this.createEntity(group);
  }

  /**
   * Perform a query of the users collection using the provided query command.
   * For example: "name contains 'ed'".
   *
   * @param ql
   * @return
   */
  public LegacyQueryResult queryGroups(final String ql) {

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
  public UsergridResponse connectEntities(final String connectingEntityType,
                                          final String connectingEntityId,
                                          final String connectionType,
                                          final String connectedEntityId) {

    return apiRequest(HTTP_POST, null, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType, connectedEntityId);
  }


  /**
   * Connect two entities together using type and name
   *
   * @param connectingEntityType
   * @param connectingEntityId
   * @param connectionType
   * @param connectedEntityName
   * @param connectedEntityType
   * @return
   */
  public UsergridResponse connectEntities(final String connectingEntityType,
                                          final String connectingEntityId,
                                          final String connectionType,
                                          final String connectedEntityType,
                                          final String connectedEntityName) {

    return apiRequest(HTTP_POST, null, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType, connectedEntityType, connectedEntityName);
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
  public UsergridResponse disconnectEntities(final String connectingEntityType,
                                             final String connectingEntityId,
                                             final String connectionType,
                                             final String connectedEntityId) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, connectingEntityType, connectingEntityId, connectionType, connectedEntityId);
  }


  /**
   * @param sourceVertex
   * @param targetVertex
   * @param connetionName
   * @return
   */
  public UsergridResponse disconnectEntities(final UsergridEntity sourceVertex,
                                             final UsergridEntity targetVertex,
                                             final String connetionName) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName,
        targetVertex.getUuid().toString());
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
  public LegacyQueryResult queryEntityConnections(final String connectingEntityType,
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
  public LegacyQueryResult queryEntityConnectionsWithinLocation(final String connectingEntityType,
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

  /**
   * Create a connection between two entities
   *
   * @param sourceVertex  The source entity/vertex of the connection
   * @param targetVertex  The target entity/vertex of the connection
   * @param connetionName The name of the connection/edge
   * @return
   */
  public UsergridResponse connectEntities(final UsergridEntity sourceVertex,
                                          final UsergridEntity targetVertex,
                                          final String connetionName) {

    return this.connectEntities(sourceVertex.getType(), sourceVertex.getUuid().toString(), connetionName, targetVertex.getUuid().toString());
  }


  public UsergridResponse queryEdgesForVertex(final String srcType,
                                              final String srcID) {

    return apiRequest(HTTP_GET, null, null, organizationId, applicationId, srcType, srcID);
  }


  public UsergridResponse queryCollections() {

    return apiRequest(HTTP_GET, null, null, this.organizationId, this.applicationId);
  }

  public UsergridResponse queryConnection(final String... segments) {

    String[] paramPath = new String[10];
    paramPath[0] = this.organizationId;
    paramPath[1] = this.applicationId;
    System.arraycopy(segments, 0, paramPath, 2, segments.length);

    return apiRequest(HTTP_GET, null, null, paramPath);
  }



  /*
   -------------------------------------
   --------- ENTITY OPERATIONS ---------
   -------------------------------------
   */

  /**
   * PUT (update) an entity, requires the type and one of (name | uuid) to be set on the entity
   *
   * @param e the entity to update
   * @return UsergridResponse
   */
  public UsergridResponse PUT(final UsergridEntity e) {

    if (isEmpty(e.getType())) {
      throw new IllegalArgumentException("UsergridEntity is required to have a 'type' property in order to to a PUT");
    }

    if (isEmpty(e.getName()) && isEmpty(e.getUuid())) {
      throw new IllegalArgumentException("UsergridEntity is required to have a 'name' or 'uuid' property in order to to a PUT");
    }

    String uuid = e.getStringProperty("uuid");

    String entityIdentifier = (uuid != null ? uuid : e.getName());

    return apiRequest(HTTP_PUT, null, e.getProperties(), organizationId, applicationId, e.getType(), entityIdentifier);
  }

  /**
   * Creates an entity using a UsergridResponse object reference
   *
   * @param usergridEntity the entity which will be created
   * @return UsergridResponse
   */
  public UsergridResponse POST(final UsergridEntity usergridEntity) {
    return this.createEntity(usergridEntity);
  }

  /**
   * Deletes the specified Entity.  Will throw an Invalid Argument if the UUID and name are null
   *
   * @param e the entity to delete.  It must have a type and UUID or name attribute on the object
   * @return UsergridResponse
   */
  public UsergridResponse DELETE(final UsergridEntity e) {
    if (e.getName() == null && e.getUuidString() == null) {
      throw new IllegalArgumentException("The entity must either have a 'name' or 'uuid' to be deleted");
    }

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, e.getType(), e.getUuid() == null ? e.getName() : e.getUuidString());
  }

    /*
   -------------------------------------
   --------- STRING OPERATIONS ---------
   -------------------------------------
   */

  /**
   * Deletes an entity (if uri is the name|uuid of the endity or a set of entities if the URI is a QL.
   *
   * @param collection the name of the collection
   * @param entityId   the ID of the entity
   * @return UsergridResponse
   */
  public UsergridResponse DELETE(final String collection,
                                 final String entityId) {

    return apiRequest(HTTP_DELETE, null, null, organizationId, applicationId, collection, entityId);
  }

  /**
   * GET an entity using the collection name and (name|uuid)
   *
   * @param collection the name of the collection
   * @param entityId   the entity ID
   * @return UsergridResponse
   */
  public UsergridResponse GET(final String collection,
                              final String entityId) {

    return apiRequest(HTTP_GET, null, null, organizationId, applicationId, collection, entityId);
  }

    /*
   -------------------------------------
   ---------- QUERY OPERATIONS ---------
   -------------------------------------
   */


  /**
   * PUT by Query
   *
   * @param q      the UsergridQuery object
   * @param fields the fields to be applied as an update to the results (entities) of the query
   * @return QueryResult
   */
  public QueryResult PUT(final UsergridQuery q, Map<String, Object> fields) {

    return new QueryResult(this,
        HTTP_PUT,
        apiRequest(HTTP_PUT, q.params(), fields, organizationId, applicationId, q.getCollectionName()),
        q,
        fields);
  }

  /**
   * GET by Query
   *
   * @param q the UsergridQuery object
   * @return QueryResult
   */
  public QueryResult GET(final UsergridQuery q) {

    return new QueryResult(this,
        HTTP_GET,
        apiRequest(HTTP_GET, q.params(), null, organizationId, applicationId, q.getCollectionName()),
        q);
  }

  /**
   * DELETE by Query
   *
   * @param q the UsergridQuery object
   * @return QueryResult
   */
  public QueryResult DELETE(final UsergridQuery q) {

    return new QueryResult(this,
        HTTP_DELETE,
        apiRequest(HTTP_DELETE, q.params(), null, organizationId, applicationId, q.getCollectionName()),
        q);
  }
}
