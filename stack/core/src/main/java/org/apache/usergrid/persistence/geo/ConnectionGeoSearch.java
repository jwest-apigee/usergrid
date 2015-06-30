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
package org.apache.usergrid.persistence.geo;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.IndexBucketLocator;
import org.apache.usergrid.persistence.cassandra.CassandraService;
import org.apache.usergrid.persistence.geo.model.Point;

import me.prettyprint.hector.api.beans.HColumn;

import static org.apache.usergrid.persistence.Schema.INDEX_CONNECTIONS;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.key;


/**
 * Class for loading connection data
 *
 * @author tnine
 */
public class ConnectionGeoSearch extends GeoIndexSearcher {

    private final UUID connectionId;


    public ConnectionGeoSearch( EntityManager entityManager,  final String shard, CassandraService cass,
                                UUID connectionId ) {
        super( entityManager, shard, cass );

        this.connectionId = connectionId;
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.usergrid.persistence.query.ir.result.GeoIterator.GeoIndexSearcher
     * #doSearch()
     */
    @Override
    protected TreeSet<HColumn<ByteBuffer, ByteBuffer>> doSearch( List<String> geoCells, UUID startId, Point searchPoint,
                                                                 String propertyName, int pageSize ) throws Exception {

        return query( key( connectionId, INDEX_CONNECTIONS, propertyName ), geoCells, searchPoint, startId, pageSize );
    }
}
