package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.UsergridResponse;

/**
 * Created by ApigeeCorporation on 7/27/15.
 */
public interface LegacyQueryResult {

  UsergridResponse getLastResponse();

  boolean more();

  LegacyQueryResult next();

  UsergridEntity first();
}

