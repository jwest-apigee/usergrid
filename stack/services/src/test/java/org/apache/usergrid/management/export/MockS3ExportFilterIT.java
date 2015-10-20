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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import org.apache.usergrid.NewOrgAppAdminRule;
import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.JobSchedulerService;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
import org.apache.usergrid.mq.Query;
import org.apache.usergrid.persistence.ConnectionRef;
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

import it.unimi.dsi.fastutil.Hash;

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
    private ExportService exportService = setup.getExportService();


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

    @After
    public void after() {
        File dir = new File(".");
        FileFilter fileFilter = new WildcardFileFilter("entities*.json");
        File[] files = dir.listFiles(fileFilter);
        for (File file : files) {
            file.delete();
        }

        fileFilter = new WildcardFileFilter("connections*.json");
        files = dir.listFiles(fileFilter);
        for (File file : files) {
            file.delete();
        }

    }

    @Test //Connections won't save when run with maven, but on local builds it will.
    public void test1000ConnectionsToSingleEntity() throws Exception {

        //setup
        String testFileName ="testConnectionsOnApplicationEndpoint.json";
        S3Export s3Export = new MockS3ExportImpl( testFileName );
        String appName = newOrgAppAdminRule.getApplicationInfo().getName();

        // intialize user object to be posted
        int numberOfEntitiesToBeWritten = 997;
        Entity[] entities = createEntities(numberOfEntitiesToBeWritten);

        for(int i = 1; i<numberOfEntitiesToBeWritten; i++){
            createConnectionsBetweenEntities( entities[0],entities[i] );
        }

//      Create Payload to be sent to export job
        Set applicationsToBeExported = new HashSet<>(  );
        applicationsToBeExported.add( appName );
        HashMap<String, Object> payload = payloadBuilder( null,applicationsToBeExported,null,null );

        //Starts export. Setups up mocks for the job executor.
        startExportJob( s3Export, payload );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        Set<File> exportedConnectionFiles = returnsEntityFilesExported("connections" ,testFileName );

        final InputStream in = new FileInputStream( exportedConnectionFiles.iterator().next() );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );
            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            HashMap jsonEntity =  (HashMap)jsonIterator.next();
            HashMap entityConnections =
                ( HashMap ) ( jsonEntity ).get( entities[0].getUuid().toString() );
            ArrayList connectionArray = (ArrayList)entityConnections.get( "testconnections" );
            //verifies that the number of connections should be equal to the number of entities written -1.
            assertEquals(numberOfEntitiesToBeWritten-1,connectionArray.size());
        }catch(Exception e){
            assertTrue(e.getMessage(),false );
        }

        finally{
            in.close();
        }
    }


    private void startExportJob( final S3Export s3Export, final HashMap<String, Object> payload ) throws Exception {UUID
        exportUUID = exportService.schedule( payload );
        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );
        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );
        exportService.doExport( jobExecution );
    }


    public JobData jobDataCreator( HashMap<String, Object> payload, UUID exportUUID, S3Export s3Export ) {
        JobData jobData = new JobData();

        jobData.setProperty( "jobName", "exportJob" );
        jobData.setProperty( "exportInfo", payload );
        jobData.setProperty( "exportId", exportUUID );
        jobData.setProperty( "s3Export", s3Export );

        return jobData;
    }

    public Map<String, Object> targetBuilder() {
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

    public Set returnsEntityFilesExported(String filenamePrefix,String filenameSuffix){

        //keep reading files until there aren't any more to read.
        int index = 1;
        File exportedFile = new File( filenamePrefix + index + filenameSuffix );

        Set<File> exportedEntityFiles = new HashSet<>(  );

        while(exportedFile.exists()) {
            exportedEntityFiles.add( exportedFile );
            index++;
            exportedFile = new File( filenamePrefix + index + filenameSuffix );
        }

        return exportedEntityFiles;
    }

    public void deleteSetOfFiles(Set<File> filesExported){
        for(File exportedFile : filesExported){
            exportedFile.delete();
        }
    }


    public Map filterBuilder(Query query,Set<String> applicationNames,Set<String> collectionNames,
                                                Set<String> connectionNames){
        Map<String, Object> filters = new HashMap<>();
        if(query != null)
            filters.put( "ql", query.toString());

        if(applicationNames != null) {
            ArrayList<String> appNameList = new ArrayList<>(  );
            for ( String appName : applicationNames ) {
                appNameList.add( appName );
            }
            filters.put( "apps", appNameList);
        }
        if(collectionNames != null) {
            ArrayList<String> collectionNamesList = new ArrayList<>(  );
            for ( String colName : collectionNames ) {
                collectionNamesList.add( colName );
            }
            filters.put( "collections",collectionNamesList);
        }
        if(connectionNames != null) {
            ArrayList<String> connectionNamesList = new ArrayList<>(  );
            for ( String  connectName  : connectionNames ) {
                connectionNamesList.add( connectName );
            }
            filters.put( "connections", connectionNamesList);

        }

        return filters;

    }

    public HashMap payloadBuilder(Query query,Set<String> applicationNames,Set<String> collectionNames,
                              Set<String> connectionNames){
        Map target = targetBuilder();
        Map filters = filterBuilder(query,applicationNames,collectionNames,connectionNames);

        HashMap payload = new HashMap<>(  );

        payload.put( "target",target );
        payload.put( "filters", filters );

        return payload;
    }

    public Entity[] createEntities(final int numberOfEntitiesToBeCreated) throws Exception{
        EntityManager em = setup.getEmf().getEntityManager( applicationId );

        // intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[numberOfEntitiesToBeCreated];

        // creates entities
        for ( int i = 0; i < numberOfEntitiesToBeCreated; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "users", userProperties );
        }
        //refresh so entities appear immediately
        setup.getEntityIndex().refresh( applicationId );

        return entity;
    }

    public ConnectionRef createConnectionsBetweenEntities (Entity entity1, Entity entity2) throws Exception{
        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        return em.createConnection( em.get( new SimpleEntityRef( "user", entity1.getUuid() ) ), "testConnections", em.get( new SimpleEntityRef( "user", entity2.getUuid() ) ) );
    }

}
