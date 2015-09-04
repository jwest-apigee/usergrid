package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.UsergridClient;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.UsergridResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/27/15.
 */
public class EntityQueryResult implements LegacyQueryResult {

  final String method;
  final Map<String, Object> params;
  final Object data;
  final String[] segments;
  final UsergridResponse lastResponse;
  final UsergridClient usergrid;



  public EntityQueryResult(final UsergridClient usergrid,
                           final UsergridResponse lastResponse,
                           final String method,
                           final Map<String, Object> params,
                           final Object data,
                           final String[] segments) {

    this.usergrid = usergrid;
    this.lastResponse = lastResponse;
    this.method = method;
    this.params = params;
    this.data = data;
    this.segments = segments;
  }

  private EntityQueryResult(final UsergridClient usergrid,
                            final UsergridResponse lastResponse,
                            final EntityQueryResult q) {

    this.usergrid = usergrid;
    this.lastResponse = lastResponse;
    method = q.method;
    params = q.params;
    data = q.data;
    segments = q.segments;
  }

  /**
   * @return the api lastResponse of the last request
   */
  public UsergridResponse getLastResponse() {
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
  public LegacyQueryResult next() {

    if (more()) {

      Map<String, Object> nextParams = null;

      if (params != null) {

        nextParams = new HashMap<>(params);

      } else {

        nextParams = new HashMap<>();
      }

      nextParams.put("cursor", lastResponse.getCursor());

      UsergridResponse nextResponse = this.usergrid.apiRequest(method, nextParams, data, segments);

      return new EntityQueryResult(this.usergrid, nextResponse, this);
    }

    return null;
  }

  @Override
  public UsergridEntity first() {
    return null;
  }

}