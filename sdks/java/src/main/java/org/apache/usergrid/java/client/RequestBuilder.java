package org.apache.usergrid.java.client;

import org.apache.usergrid.java.client.response.UsergridResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 9/2/15.
 */
public class RequestBuilder {

  public String collection;
  public String name;
  public Map<String, String> headers;
  public Map<String, String> parameters;
  public String uriSuffix;

  public String buildURI() {
    return "";
  }

  public RequestBuilder collection(final String collection) {
    this.collection = collection;
    return this;
  }

  public RequestBuilder entity(final String name_uuid) {
    this.name = name_uuid;
    return this;
  }

  public RequestBuilder parameter(final String name, final String value) {

    return this;
  }

  public RequestBuilder header(final String name, final String value) {

    if (this.headers == null) {
      this.headers = new HashMap<>(13);
    }

    this.headers.put(name, value);

    return this;
  }

  public RequestBuilder headers(final Map<String, String> headers) {
    this.headers = headers;
    return this;
  }

  public RequestBuilder parameters(final Map<String, String> parameters) {
    this.parameters = parameters;
    return this;
  }


  public UsergridResponse GET() {
    String uri = buildURI();

//    return Usergrid.getInstance().GET();
    return null;
  }

  public UsergridResponse GET(String uri) {
    return null;
  }
}
