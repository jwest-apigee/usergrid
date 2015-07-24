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

import static org.apache.usergrid.java.client.utils.JsonUtils.setStringProperty;
import static org.apache.usergrid.java.client.utils.JsonUtils.*;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.usergrid.java.client.Client;


/**
 * Created by ayeshadastagiri on 7/2/15.
 */
public class Connection {

  public final static String LABEL = "label";
  public final static String PROPERTY_ID = "connectionId";

  protected Map<String, JsonNode> properties = new HashMap<String, JsonNode>();

  UsergridEntity source;
  UsergridEntity target;

  Client client;

  public Connection(UsergridEntity source, String label, UsergridEntity target) {
    this.source = source;
    this.target = target;
    this.setLabel(label);
  }

  protected Connection() {

  }

  public void setLabel(String label) {
    setStringProperty(properties, LABEL, label);
  }

  public void setConnectionID(String connID) {
    setStringProperty(properties, PROPERTY_ID, connID);
  }

  public String getLabel() {
    return getStringProperty(properties, LABEL);
  }


  public void setClientConnection(Client client) {
    this.client = client;
  }

  public Client getClientConnection() {
    return this.client;
  }

  public String getPropertyId() {
    return getStringProperty(properties, PROPERTY_ID);
  }

  public long getTimestamp() {
    return 0;
  }

}
