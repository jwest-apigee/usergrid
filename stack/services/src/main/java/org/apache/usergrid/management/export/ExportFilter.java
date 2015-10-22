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
 * A model of the filters included in usergrid and how they can be added to.
 */
public interface ExportFilter {
    public Set getApplications();

    public Set getCollections();

    public Set getConnections();

    public Query getQuery();

    public void setApplications(Set applications);

    public void setCollections(Set collections);

    public void setConnections(Set connections);

    public void setQuery(Query query);

}
