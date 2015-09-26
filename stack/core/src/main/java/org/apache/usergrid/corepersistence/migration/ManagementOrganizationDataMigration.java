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

package org.apache.usergrid.corepersistence.migration;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.usergrid.corepersistence.util.CpNamingUtils;
import org.apache.usergrid.persistence.*;
import org.apache.usergrid.persistence.cassandra.CassandraService;
import org.apache.usergrid.persistence.core.migration.data.DataMigration;
import org.apache.usergrid.persistence.core.migration.data.ProgressObserver;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.entities.Group;
import static org.apache.usergrid.persistence.Schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


@Singleton
public class ManagementOrganizationDataMigration implements DataMigration {

    private static final Logger logger = LoggerFactory.getLogger(ManagementOrganizationDataMigration.class);

    private final EntityManagerFactory entityManagerFactory;
    private final ManagementService managementService;

    @Inject
    public ManagementOrganizationDataMigration(final EntityManagerFactory entityManagerFactory, final ManagementService managementService) {
        this.entityManagerFactory = entityManagerFactory;
        this.managementService = managementService;
    }


    @Override
    public int migrate( final int currentVersion, final ProgressObserver observer ) {

        final int migrationVersion = getMaxVersion();

        observer.start();

        EntityManager em = entityManagerFactory.getEntityManager(CpNamingUtils.MANAGEMENT_APPLICATION_ID);

        UUID managementAppId = entityManagerFactory.getManagementAppId();
        UUID managementOrgId = entityManagerFactory.getManagementOrgId();
        String managementAppName = CassandraService.MANAGEMENT_APPLICATION;
        String managementOrgName = CassandraService.DEFAULT_ORGANIZATION;
        String managementOrgPlusAppName = CassandraService.MANAGEMENT_ORGPLUSAPP;
        Application app = null;

        // make sure management app exists
        try {
            app = em.getApplication();
            if (app == null) {
                // create the management application
                entityManagerFactory.createApplicationV2(managementOrgName, managementAppName,
                        managementAppId, null);
                app = em.getApplication();
            } else if (!managementAppName.equals(app.getName())) {
                logger.info(String.format("Management application name is %s, should be %s", app.getName(), managementOrgPlusAppName));
                observer.update(migrationVersion, String.format("Management application name is %s, should be %s", app.getName(), managementOrgPlusAppName));
            }
        } catch (Exception e) {
            // should never happen -- abort
            logger.error("MANAGEMENT APPLICATION ERROR", e);
            observer.failed(migrationVersion, String.format("MANAGEMENT APPLICATION ERROR: %s", e.getMessage()));
            return currentVersion;
        }

        // check for existence of management org, and create if necessary
        boolean createdOrg = false;
        try {
            if (!em.isPropertyValueUniqueForEntity(Group.ENTITY_TYPE, PROPERTY_PATH, managementOrgName)) {
                // organization already exists
                logger.info(String.format("Organization with name %s already exists", managementOrgName));
                observer.update(migrationVersion, String.format("Management organization %s already exists", managementOrgName));
            } else if (!em.isPropertyValueUniqueForEntity(Group.ENTITY_TYPE, PROPERTY_UUID, managementOrgId)) {
                // organization does not have correct UUID
                logger.error("Organization that has management organization UUID does not have correct organization name");
                observer.failed(migrationVersion, "Organization that has management organization UUID does not have correct organization name");
                return migrationVersion;
            } else {
                // create management org
                Group organizationEntity = new Group();
                organizationEntity.setPath(managementOrgName);
                em.create(managementOrgId, Group.ENTITY_TYPE, organizationEntity.getProperties());
                logger.info("Management org created");
                createdOrg = true;
            }
        } catch (Exception e) {
            // error with management org
            logger.error("MANAGEMENT ORGANIZATION ERROR", e);
            observer.update(migrationVersion, String.format("MANAGEMENT ORGANIZATION ERROR: %s", e.getMessage()));
        }

        // add org property to app
        if (createdOrg && app != null && !managementOrgName.equals(app.getProperty("org"))) {
            try {
                app.setProperty("org", managementOrgName);
                em.update(app);
                logger.info("Management app updated with management org");
            } catch (Exception e) {
                // error linking management app to management org
                logger.error("ERROR ADDING MANAGEMENT ORG TO MANAGEMENT APP", e);
                observer.update(migrationVersion, String.format("ERROR ADDING MANAGEMENT ORG TO MANAGEMENT APP: %s", e.getMessage()));
            }
        }

        // add connection
        if (app != null) {
            if (managementService)
            Results r = em.getSourceEntities(new SimpleEntityRef(CpNamingUtils.APPLICATION_INFO, managementAppId),
                    ORG_APP_RELATIONSHIP, Group.ENTITY_TYPE, Query.Level.ALL_PROPERTIES);
            Entity entity = r.getEntity();
            if (entity == null) {
                // add the connection
            }
        }


        observer.complete();

        return migrationVersion;

    }


    @Override
    public boolean supports( final int currentVersion ) {
        return currentVersion == getMaxVersion() - 1;
    }


    @Override
    public int getMaxVersion() {
        return 3;
    }
}
