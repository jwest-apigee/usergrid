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
package org.apache.usergrid.management.export;


import java.util.Set;

import org.apache.usergrid.mq.Query;


/**
 * Implementation that also parses the json data to get the filter information.
 */
public class ExportFilterImpl implements ExportFilter {
    public Query query;

    private Set applications;

    private Set collections;

    private Set connections;

    @Override
    public Set getApplications() {
        return applications;
    }


    @Override
    public Set getCollections() {
        return collections;
    }


    @Override
    public Set getConnections() {
        return connections;
    }


    @Override
    public Query getQuery() {
        return query;
    }


    @Override
    public void setApplications( final Set applications ) {
        this.applications = applications
    }


    @Override
    public void setCollections( final Set collections ) {
        this.collections = collections;
    }


    @Override
    public void setConnections( final Set connections ) {
        this.connections = connections;
    }


    @Override
    public void setQuery( final Query query ) {
        this.query = query;
    }
}
