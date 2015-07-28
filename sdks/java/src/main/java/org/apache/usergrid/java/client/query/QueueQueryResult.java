package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.response.ApiResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/27/15.
 */
public class QueueQueryResult implements QueryResult {
  private Usergrid usergrid;
  final String method;
  final Map<String, Object> params;
  final Object data;
  final String queuePath;
  final ApiResponse response;

  public QueueQueryResult(final Usergrid usergrid,
                          final ApiResponse response,
                          final String method,
                          final Map<String, Object> params,
                          final Object data,
                          final String queuePath) {

    this.usergrid = usergrid;
    this.response = response;
    this.method = method;
    this.params = params;
    this.data = data;
    this.queuePath = usergrid.normalizeQueuePath(queuePath);
  }

  public QueueQueryResult(final Usergrid usergrid,
                           final ApiResponse response,
                           final QueueQueryResult q) {

    this.usergrid = usergrid;
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
      ApiResponse nextResponse = usergrid.apiRequest(method, nextParams, data, queuePath);

      return new QueueQueryResult(this.usergrid, nextResponse, this);
    }

    return null;
  }

}
