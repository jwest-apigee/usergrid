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
package org.apache.usergrid.java.client.utils;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.usergrid.java.client.exception.ClientException;

public class JsonUtils {


  static ObjectMapper mapper = new ObjectMapper();

  public static String getStringProperty(final Map<String, JsonNode> properties,
                                         final String name) {

    JsonNode value = properties.get(name);

    if (value != null) {
      return value.asText();
    }

    return null;
  }

  public static void setStringProperty(final Map<String, JsonNode> properties,
                                       final String name,
                                       final String value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, JsonNodeFactory.instance.textNode(value));
    }
  }

  public static Long getLongProperty(final Map<String, JsonNode> properties,
                                     final String name) {
    JsonNode value = properties.get(name);

    if (value != null) {
      return value.asLong(0);
    }

    return null;
  }

  public static void setLongProperty(final Map<String, JsonNode> properties,
                                     final String name,
                                     final Long value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, JsonNodeFactory.instance.numberNode(value));
    }
  }

  public static void setFloatProperty(final Map<String, JsonNode> properties,
                                      final String name,
                                      final Float value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, JsonNodeFactory.instance.numberNode(value));
    }
  }

  public static Boolean getBooleanProperty(final Map<String, JsonNode> properties,
                                           final String name) {
    JsonNode value = properties.get(name);
    return value != null && value.asBoolean();
  }

  public static void setBooleanProperty(final Map<String, JsonNode> properties,
                                        final String name, Boolean value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, JsonNodeFactory.instance.booleanNode(value));
    }
  }

  public static UUID getUUIDProperty(final Map<String, JsonNode> properties,
                                     final String name) {
    JsonNode value = properties.get(name);
    if (value != null) {
      UUID uuid = null;
      try {
        uuid = UUID.fromString(value.asText());
      } catch (Exception ignore) {
      }
      return uuid;
    }
    return null;
  }

  public static void setUUIDProperty(final Map<String, JsonNode> properties,
                                     final String name,
                                     final UUID value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name,
          JsonNodeFactory.instance.textNode(value.toString()));
    }
  }

  public static String toJsonString(final Object obj) {
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonGenerationException e) {
      throw new ClientException("Unable to generate json", e);
    } catch (JsonMappingException e) {
      throw new ClientException("Unable to map json", e);
    } catch (IOException e) {
      throw new ClientException("IO error", e);
    }
  }

  public static <T> T parse(final String json,
                            final Class<T> c) {
    try {
      return mapper.readValue(json, c);
    } catch (JsonGenerationException e) {
      throw new ClientException("Unable to generate json", e);
    } catch (JsonMappingException e) {
      throw new ClientException("Unable to map json", e);
    } catch (IOException e) {
      throw new ClientException("IO error", e);
    }
  }

  public static JsonNode toJsonNode(final Object obj) {
    return mapper.convertValue(obj, JsonNode.class);
  }

  public static <T> T fromJsonNode(final JsonNode json,
                                   final Class<T> c) {
    try {
      JsonParser jp = json.traverse();
      return mapper.readValue(jp, c);
    } catch (JsonGenerationException e) {
      throw new ClientException("Unable to generate json", e);
    } catch (JsonMappingException e) {
      throw new ClientException("Unable to map json", e);
    } catch (IOException e) {
      throw new ClientException("IO error", e);
    }
  }

  public static <T> T getObjectProperty(final Map<String, JsonNode> properties,
                                        final String name,
                                        final Class<T> c) {
    JsonNode value = properties.get(name);
    if (value != null) {
      return fromJsonNode(value, c);
    }
    return null;
  }

  public static void setObjectProperty(final Map<String, JsonNode> properties,
                                       final String name,
                                       final ObjectNode value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.put(name, value);
    }
  }

  public static void setObjectProperty(final Map<String, JsonNode> properties,
                                       final String name,
                                       final Object value) {
    if (value == null) {
      properties.remove(name);
    } else if (value instanceof JsonNode) {
      properties.put(name, (JsonNode) value);
    } else {
      properties.put(name, JsonNodeFactory.instance.textNode(value.toString()));
    }
  }

  public static <T> T getProperty(final Map<String, JsonNode> properties,
                                  final String name) {
    JsonNode value = properties.get(name);

    if (value instanceof TextNode) {
      return (T) value.asText();
    } else if (value instanceof LongNode) {
      Long valueLong = value.asLong();
      return (T) valueLong;
    } else if (value instanceof BooleanNode) {
      Boolean valueBoolean = value.asBoolean();
      return (T) valueBoolean;
    } else if (value instanceof IntNode) {
      Integer valueInteger = value.asInt();
      return (T) valueInteger;
    } else if (value instanceof FloatNode) {
      return (T) Float.valueOf(value.toString());
    } else {
      return (T) value;
    }
  }

}
