package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.ApiResponse;

/**
 * Created by ApigeeCorporation on 7/27/15.
 */
public interface QueryResult  {

  ApiResponse getLastResponse();

  boolean more();

  QueryResult next();

  UsergridEntity first();
}

