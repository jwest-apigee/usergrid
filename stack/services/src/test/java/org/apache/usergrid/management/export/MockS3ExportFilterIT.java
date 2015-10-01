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


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.NewOrgAppAdminRule;
import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.JobSchedulerService;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
import org.apache.usergrid.mq.Query;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.SimpleEntityRef;
import org.apache.usergrid.persistence.entities.JobData;
import org.apache.usergrid.services.AbstractServiceIT;
import org.apache.usergrid.setup.ConcurrentProcessSingleton;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests filters in export v2.
 */
public class MockS3ExportFilterIT extends AbstractServiceIT {


    private static final Logger logger = LoggerFactory.getLogger( MockS3ExportFilterIT.class );

    @Rule
    public NewOrgAppAdminRule newOrgAppAdminRule = new NewOrgAppAdminRule( setup );



    // app-level data generated only once
    private UserInfo adminUser;
    private OrganizationInfo organization;
    private UUID applicationId;

    @Before
    public void setup() throws Exception {
        logger.info( "in setup" );

        // start the scheduler after we're all set up
        try {

            JobSchedulerService jobScheduler =
                ConcurrentProcessSingleton.getInstance().getSpringResource().getBean( JobSchedulerService.class );
            if ( jobScheduler.state() != Service.State.RUNNING ) {
                jobScheduler.startAsync();
                jobScheduler.awaitRunning();
            }
        }
        catch ( Exception e ) {
            logger.warn( "Ignoring error starting jobScheduler, already started?", e );
        }

        adminUser = newOrgAppAdminRule.getAdminInfo();
        organization = newOrgAppAdminRule.getOrganizationInfo();
        applicationId = newOrgAppAdminRule.getApplicationInfo().getId();

        setup.getEntityIndex().refresh( applicationId );
    }


    @Before
    public void before() {
        adminUser = newOrgAppAdminRule.getAdminInfo();
        organization = newOrgAppAdminRule.getOrganizationInfo();
        applicationId = newOrgAppAdminRule.getApplicationInfo().getId();
    }


    @Test //Connections won't save when run with maven, but on local builds it will.
    public void test1000ConnectionsToSingleEntity() throws Exception {

        String testFileName ="testConnectionsOnApplicationEndpoint.json";

        S3Export s3Export = new MockS3ExportImpl( testFileName );

        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );

        EntityManager em = setup.getEmf().getEntityManager( applicationId );

        // intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        int numberOfEntitiesToBeWritten = 997;
        entity = new Entity[numberOfEntitiesToBeWritten];

        // creates entities
        for ( int i = 0; i < numberOfEntitiesToBeWritten; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "users", userProperties );
        }

        for(int i = 1; i<numberOfEntitiesToBeWritten; i++){
            em.createConnection( em.get( new SimpleEntityRef( "user", entity[0].getUuid() ) ), "testConnections",
                em.get( new SimpleEntityRef( "user", entity[i].getUuid() ) ) );
        }

        setup.getEntityIndex().refresh( applicationId );

        Thread.sleep( 1000 );

        UUID exportUUID = exportService.schedule( payload );

        //create and initialize jobData returned in JobExecution.
        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        File exportedFile = new File("entities1"+testFileName);
        exportedFile.delete();
        exportedFile = new File("entities2"+testFileName);
        exportedFile.delete();

        final InputStream in = new FileInputStream( "connections1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );
            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            HashMap jsonEntity =  (HashMap)jsonIterator.next();
            HashMap entityConnections =
                ( HashMap ) ( jsonEntity ).get( entity[0].getUuid().toString() );
            ArrayList connectionArray = (ArrayList)entityConnections.get( "testconnections" );
            assertEquals(numberOfEntitiesToBeWritten-1,connectionArray.size());
        }catch(Exception e){
            assertTrue(e.getMessage(),false );
        }

        finally{
            in.close();
        }

        // Delete the created connection files
        exportedFile = new File("connections1"+testFileName);
        exportedFile.delete();
        exportedFile = new File("connections2"+testFileName);
        exportedFile.delete();
    }

    public JobData jobDataCreator( HashMap<String, Object> payload, UUID exportUUID, S3Export s3Export ) {
        JobData jobData = new JobData();

        jobData.setProperty( "jobName", "exportJob" );
        jobData.setProperty( "exportInfo", payload );
        jobData.setProperty( "exportId", exportUUID );
        jobData.setProperty( "s3Export", s3Export );

        return jobData;
    }


    /*Creates fake payload for testing purposes.*/
    public HashMap<String, Object> payloadBuilder( String orgOrAppName ) {
        HashMap<String, Object> payload = new HashMap<String, Object>();
        Map<String, Object> target = new HashMap<String, Object>();
        Map<String, Object> storage_info = new HashMap<String, Object>();
        storage_info.put( "s3_key", "null");
        storage_info.put( "s3_access_id", "null" );
        storage_info.put( "bucket_location", "null" );

        target.put( "storage_provider", "s3" );
        target.put( "storage_info", storage_info );

        payload.put( "target", target );
        return payload;
    }

    public HashMap<String, Object> targetBuilder() {
        HashMap<String, Object> payload = new HashMap<String, Object>();
        Map<String, Object> target = new HashMap<String, Object>();
        Map<String, Object> storage_info = new HashMap<String, Object>();
        storage_info.put( "s3_key", "null");
        storage_info.put( "s3_access_id", "null" );
        storage_info.put( "bucket_location", "null" );

        target.put( "storage_provider", "s3" );
        target.put( "storage_info", storage_info );

        payload.put( "target", target );
        return payload;
    }


    public HashMap<String, Object> filterBuilder(Query query,Set<String> applicationNames,Set<String> collectionNames,
                                                Set<String> connectionNames){
        Map<String, Object> filters = new HashMap<>();
        if(query != null)
            filters.put( "ql", query.toString());

        if(applicationNames != null) {
            for ( String appName : applicationNames ) {
                filters.put( "apps", < put array of garbage here>)
            }
        }
        if(collectionNames != null) {
            for ( String colName : collectionNames ) {
                filters.put( "collections", < put array of garbage here>)
            }
        }
        if(connectionNames != null) {
            for ( String  connectionNames  : connectionNames ) {
                filters.put( "connections", < put array of garbage here>)
            }
        }

        return 


    }

}
