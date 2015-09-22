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
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.http.config.JavaUrlHttpCommandExecutorServiceModule;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.netty.config.NettyPayloadModule;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.usergrid.NewOrgAppAdminRule;
import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.JobSchedulerService;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.SimpleEntityRef;
import org.apache.usergrid.persistence.core.aws.NoAWSCredsRule;
import org.apache.usergrid.persistence.entities.JobData;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.services.AbstractServiceIT;
import org.apache.usergrid.services.ServiceAction;
import org.apache.usergrid.setup.ConcurrentProcessSingleton;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import com.google.inject.Module;

import static org.apache.usergrid.TestHelper.newUUIDString;
import static org.apache.usergrid.TestHelper.uniqueApp;
import static org.apache.usergrid.TestHelper.uniqueOrg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Export Service Tests that don't need to connect to s3 to verify that export format is working.
 */
public class MockExportServiceIT extends AbstractServiceIT {

    private static final Logger logger = LoggerFactory.getLogger( ExportServiceIT.class );

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


    //Tests to make sure we can call the job with mock data and it runs.
    //@Ignore( "Connections won't save when run with maven, but on local builds it will." )
    @Test
    public void testConnectionsOnCollectionExport() throws Exception {

        File f = null;
        int indexCon = 0;

        try {
            f = new File( "testFileConnections.json" );
        }
        catch ( Exception e ) {
            // consumed because this checks to see if the file exists.
            // If it doesn't then don't do anything and carry on.
        }
        f.deleteOnExit();

        S3Export s3Export = new MockS3ExportImpl( "testFileConnections.json" );

        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );
        payload.put( "collectionName", "users" );

        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        //intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[2];
        //creates entities
        for ( int i = 0; i < 2; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "meatIsGreat" + i );
            userProperties.put( "email", "grey" + i + "@anuff.com" );//String.format( "test%i@anuff.com", i ) );

            entity[i] = em.create( "users", userProperties );
        }
        //creates connections
        em.createConnection( em.get( new SimpleEntityRef( "user", entity[0].getUuid() ) ), "Vibrations",
            em.get( new SimpleEntityRef( "user", entity[1].getUuid() ) ) );
        em.createConnection( em.get( new SimpleEntityRef( "user", entity[1].getUuid() ) ), "Vibrations",
            em.get( new SimpleEntityRef( "user", entity[0].getUuid() ) ) );

        UUID exportUUID = exportService.schedule( payload );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        List usersList = ( List ) collectionsMap.get( "users" );

        int indexApp = 0;
        for ( indexApp = 0; indexApp < usersList.size(); indexApp++ ) {
            Map user = ( Map ) usersList.get( indexApp );
            Map userProps = ( Map ) user.get( "Metadata" );
            String uuid = ( String ) userProps.get( "uuid" );
            if ( entity[0].getUuid().toString().equals( uuid ) ) {
                break;
            }
        }

        assertTrue( "Uuid was not found in exported files. ", indexApp < usersList.size() );

        Map userMap = ( Map ) usersList.get( indexApp );
        Map connectionsMap = ( Map ) userMap.get( "connections" );
        assertNotNull( connectionsMap );

        List vibrationsList = ( List ) connectionsMap.get( "Vibrations" );

        assertNotNull( vibrationsList );

        f.deleteOnExit();
    }


    //What is required to make this test a reality?
    //Second we need a way to delete the file and bucket out of s3 once data has been verified.
//    @Test
//    public void testConnectionsOnApplicationExport() throws Exception {
//        //Populate a application with data that contains connections.
//        ExportService exportService = setup.getExportService();
//
//        ApplicationInfo appMade = setup.getMgmtSvc().getApplicationInfo(
//            app.getOrgName() + "/" + app.getAppName().toLowerCase() );
//        String orgName = setup.getMgmtSvc().getOrganizations().values().iterator().next();
//
//        OrganizationInfo orgMade = setup.getMgmtSvc().getOrganizationByName( orgName );
//
//
//        EntityManager em = setup.getEmf().getEntityManager( appMade.getId() );
//        em.createApplicationCollection( "testConnectionsOnApplicationCol" );
//
//        Entity[] entity;
//        entity = new Entity[2];
//
//        for ( int i = 0; i < 2; i++ ) {
//            app.put( "username", "billybob" + RandomStringUtils.randomAlphabetic( 5 ).toLowerCase() );
//            app.put( "email", "test" + RandomStringUtils.randomAlphabetic( 5 ).toLowerCase() + i + "@anuff.com" );
//
//            entity[i] = app.testRequest( ServiceAction.POST, 1, "users" ).getEntity();
//            app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[i] ).getUsername() );
//        }
//
//        //POST users/conn-user1/manages/user2/conn-user2
//        app.testRequest( ServiceAction.POST, 1, "users", ( ( User ) entity[0] ).getUsername(), "manages", "users",
//            ( ( User ) entity[1] ).getUsername() );
//        app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[0] ).getUsername() );
//
//        //POST users/conn-user1/reports/users/conn-user2
//        app.testRequest( ServiceAction.POST, 1, "users", ( ( User ) entity[1] ).getUsername(), "relates", "users",
//            ( ( User ) entity[0] ).getUsername() );
//        Entity user2 = app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[1] ).getUsername() ).getEntity();
//
//        setup.refreshIndex( appMade.getId() );
//
//        HashMap<String, Object> payload = payloadBuilder( appMade.getName() );
//
//        payload.put( "organizationId", orgMade.getUuid() );
//        payload.put("applicationId",appMade.getId());
//
//        //this kicks off the actual export with a unique bucket and filename.
//        UUID exportUUID = exportService.schedule( payload );
//        assertNotNull( exportUUID );
//
//        //Anything around two seconds isn't enough for s3 so I bumped it up to 4 seconds to be safe.
//        Thread.sleep( 4000 );
//
//        logger.info( "Downloading an object" );
//
//        Set<String> s3ObjectList = returnObjectListsFromBucket();
//        assertNotEquals( 0, s3ObjectList.size() );
//
//
//        for ( String s3ObjectKey : s3ObjectList ) {
//
//            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
//
//            S3ObjectInputStream s3ObjectInputStream = s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();
//
//            ObjectMapper mapper = new ObjectMapper();
//            Map<String, Object> exportJsonEntitiesMap = mapper.readValue( s3ObjectInputStream, typeRef );
//
//            assertNotNull( exportJsonEntitiesMap );
//
//            Map collectionsMap = ( Map ) exportJsonEntitiesMap.get( "collections" );
//            String collectionName = ( String ) collectionsMap.keySet().iterator().next();
//            List collection = ( List ) collectionsMap.get( "users" );
//
//            for ( Object o : collection ) {
//                Map entityMap = ( Map ) o;
//                Map metadataMap = ( Map ) entityMap.get( "connections" );
//                assertNotEquals( "Connections weren't saved in the export process.", 0, metadataMap.size() );
//            }
//        }
//    }


    //Second we need a way to delete the file and bucket out of s3 once data has been verified.
//    @Test
//    public void testApplicationExport() throws Exception {
//        //Populate a application with data that contains connections.
//        ExportService exportService = setup.getExportService();
//
//        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
//        HashMap<String, Object> payload = payloadBuilder( appName );
//
//        OrganizationInfo orgMade = null;
//        ApplicationInfo appMade = null;
//        for ( int i = 0; i < 5; i++ ) {
//            orgMade = setup.getMgmtSvc().createOrganization( "minorboss" + i, adminUser, true );
//            for ( int j = 0; j < 5; j++ ) {
//                appMade = setup.getMgmtSvc().createApplication( orgMade.getUuid(), "superapp" + j );
//
//                EntityManager customMaker = setup.getEmf().getEntityManager( appMade.getId() );
//                customMaker.createApplicationCollection( "superappCol" + j );
//                //intialize user object to be posted
//                Map<String, Object> entityLevelProperties = null;
//                Entity[] entNotCopied;
//                entNotCopied = new Entity[5];
//                //creates entities
//                for ( int index = 0; index < 5; index++ ) {
//                    entityLevelProperties = new LinkedHashMap<String, Object>();
//                    entityLevelProperties.put( "derp", "bacon" );
//                    entNotCopied[index] = customMaker.create( "superappCol" + j, entityLevelProperties );
//                }
//            }
//        }
//
//        payload.put( "organizationId", orgMade.getUuid() );
//
//        //this kicks off the actual export with a unique bucket and filename.
//        UUID exportUUID = exportService.schedule( payload );
//        assertNotNull( exportUUID );
//
//        //Anything around two seconds isn't enough for s3 so I bumped it up to 5 seconds to be safe.
//        Thread.sleep( 5000 );
//
//        logger.info( "Downloading an object" );
//
//        Set<String> s3ObjectList = returnObjectListsFromBucket();
//        assertNotEquals( 0, s3ObjectList.size() );
//
//
//        for ( String s3ObjectKey : s3ObjectList ) {
//
//            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
//
//            S3ObjectInputStream s3ObjectInputStream = s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();
//
//            ObjectMapper mapper = new ObjectMapper();
//            Map<String, Object> exportJsonEntitiesMap = mapper.readValue( s3ObjectInputStream, typeRef );
//
//            assertNotNull( exportJsonEntitiesMap );
//
//            Map collectionsMap = ( Map ) exportJsonEntitiesMap.get( "collections" );
//            String collectionName = ( String ) collectionsMap.keySet().iterator().next();
//            List collection = ( List ) collectionsMap.get( collectionName );
//
//            for ( Object o : collection ) {
//                Map entityMap = ( Map ) o;
//                Map metadataMap = ( Map ) entityMap.get( "Metadata" );
//                String entityName = ( String ) metadataMap.get( "derp" );
//                assertTrue( "derp doesn't contain bacon? What??", entityName.equals( "bacon" ) );
//            }
//        }
//
//        //do what you did below as that is how they are stored. Look at the bucket and make sure its randomized.
//
//        //verify that all objects are still there
//    }


    @Test //Connections won't save when run with maven, but on local builds it will.
    public void test2001EntitiesOnApplicationEndpoint() throws Exception {

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
        int numberOfEntitiesToBeWritten = 999;
        entity = new Entity[numberOfEntitiesToBeWritten];

        // creates entities
        for ( int i = 0; i < numberOfEntitiesToBeWritten; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "users", userProperties );
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

        int validEntitiesCounter = 0;
        final InputStream in = new FileInputStream( "entities1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );


            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            while(jsonIterator.hasNext()){
                Object jsonEntity =  jsonIterator.next();
                assertNotNull( ( ( HashMap ) jsonEntity ).get( "uuid" ));
                validEntitiesCounter++;
            }
        }finally{
            in.close();
//            File exportedFile = new File("entities1"+testFileName);
//            exportedFile.delete();
        }

        //need to add three to all verifies because of the three default roles.
        assertEquals( "There should have been "+(numberOfEntitiesToBeWritten+3)+" valid entities in the file",numberOfEntitiesToBeWritten+3,validEntitiesCounter );

        //Read the connection entities and verify that they are correct

//        int validConnectionCounter = 0;
//        final InputStream connectionsInputStream = new FileInputStream( "connections1"+testFileName );
//        try{
//            ObjectMapper mapper = new ObjectMapper();
//            JsonParser jp = new JsonFactory(  ).createParser( connectionsInputStream );
//
//            Iterator jsonIterator = mapper.readValues( jp, typeRef);
//            while(jsonIterator.hasNext()){
//                Object jsonConnection =  jsonIterator.next();
//                if(( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() )!=null){
//                    Object jsonEntityConnection = ( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() );
//                    assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );
//
//                    //String is stored in an array so we need to trim array fixings off the end of the string to compare.
//                    String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
//                    connectionUuid=connectionUuid.substring( 1,connectionUuid.length()-1 );
//                    if(connectionUuid.equals( entity[0].getUuid().toString() ))
//                        validConnectionCounter++;
//                }
//                else if(( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString() )!=null){
//                    Object jsonEntityConnection = ( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString());
//                    assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );
//
//                    String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
//                    connectionUuid=connectionUuid.substring( 1,connectionUuid.length()-1 );
//
//                    if(connectionUuid.equals( entity[1].getUuid().toString() ))
//                        validConnectionCounter++;
//                }
//            }
//        }finally{
//            in.close();
            File exportedFile = new File("connections1"+testFileName);
            exportedFile.delete();
//        }
//
//        assertEquals( "There should have been two valid connections in the file",2,validConnectionCounter );
    }

    @Test //Connections won't save when run with maven, but on local builds it will.
    public void test1000EntitiesPerFile() throws Exception {

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
        int numberOfEntitiesToBeWritten = 1100;
        entity = new Entity[numberOfEntitiesToBeWritten];

        // creates entities
        for ( int i = 0; i < numberOfEntitiesToBeWritten; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "users", userProperties );
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

        int validEntitiesCounter = 0;
        final InputStream in = new FileInputStream( "entities1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );


            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            while(jsonIterator.hasNext()){
                Object jsonEntity =  jsonIterator.next();
                assertNotNull( ( ( HashMap ) jsonEntity ).get( "uuid" ));
                validEntitiesCounter++;
            }
        }finally{
            in.close();
            File exportedFile = new File("entities1"+testFileName);
            exportedFile.delete();
            exportedFile = new File("entities2"+testFileName);
            exportedFile.delete();
        }

        //need to add three to all verifies because of the three default roles.
        assertEquals( "There should have been 1000 valid entities in the file",1000,validEntitiesCounter );

        //Delete the created connection files
        File exportedFile = new File("connections1"+testFileName);
        exportedFile.delete();
        exportedFile = new File("connections2"+testFileName);
        exportedFile.delete();
    }

    @Test //Connections won't save when run with maven, but on local builds it will.
    public void testConnectionsOnApplicationEndpoint() throws Exception {

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
        entity = new Entity[2];

        // creates entities
        for ( int i = 0; i < 2; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );//String.format( "test%i@anuff.com", i ) );

            entity[i] = em.create( "users", userProperties );
        }
        //creates connections
        em.createConnection( em.get( new SimpleEntityRef( "user", entity[0].getUuid() ) ), "Vibrations",
            em.get( new SimpleEntityRef( "user", entity[1].getUuid() ) ) );
        em.createConnection( em.get( new SimpleEntityRef( "user", entity[1].getUuid() ) ), "Vibrations",
            em.get( new SimpleEntityRef( "user", entity[0].getUuid() ) ) );

        setup.getEntityIndex().refresh( applicationId );

        Thread.sleep( 1000 );

        UUID exportUUID = exportService.schedule( payload );

        //create and initialize jobData returned in JobExecution.
        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        int validEntitiesCounter = 0;
        final InputStream in = new FileInputStream( "entities1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );


            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            while(jsonIterator.hasNext()){
                Object jsonEntity =  jsonIterator.next();
                if(( ( HashMap ) jsonEntity ).get( "uuid" ).equals( entity[0].getUuid().toString())){
                    validEntitiesCounter++;
                }
                else if(( ( HashMap ) jsonEntity ).get( "uuid" ).equals(entity[1].getUuid().toString())){
                    validEntitiesCounter++;
                }
            }
        }finally{
            in.close();
            File exportedFile = new File("entities1"+testFileName);
            exportedFile.delete();
        }

        assertEquals( "There should have been two valid entities in the file",2,validEntitiesCounter );

        //Read the connection entities and verify that they are correct

        int validConnectionCounter = 0;
        final InputStream connectionsInputStream = new FileInputStream( "connections1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( connectionsInputStream );

            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            while(jsonIterator.hasNext()){
                Object jsonConnection =  jsonIterator.next();
                if(( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() )!=null){
                    Object jsonEntityConnection = ( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() );
                    assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );

                    //String is stored in an array so we need to trim array fixings off the end of the string to compare.
                    String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
                    connectionUuid=connectionUuid.substring( 1,connectionUuid.length()-1 );
                    if(connectionUuid.equals( entity[0].getUuid().toString() ))
                        validConnectionCounter++;
                }
                else if(( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString() )!=null){
                    Object jsonEntityConnection = ( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString());
                    assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );

                    String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
                    connectionUuid=connectionUuid.substring( 1,connectionUuid.length()-1 );

                    if(connectionUuid.equals( entity[1].getUuid().toString() ))
                        validConnectionCounter++;
                }
            }
        }finally{
            in.close();
            File exportedFile = new File("connections1"+testFileName);
            exportedFile.delete();
        }

        assertEquals( "There should have been two valid connections in the file",2,validConnectionCounter );
    }


    @Test
    public void testExportOneOrgCollectionEndpoint() throws Exception {

        File f = null;

        try {
            f = new File( "exportOneOrg.json" );
        }
        catch ( Exception e ) {
            //consumed because this checks to see if the file exists.
            // If it doesn't then don't do anything and carry on.
        }
        f.deleteOnExit();


        //create another org to ensure we don't export it
        newOrgAppAdminRule.createOwnerAndOrganization( "noExport" + newUUIDString(), "junkUserName" + newUUIDString(),
            "junkRealName" + newUUIDString(), newUUIDString() + "ugExport@usergrid.com", "123456789" );

        S3Export s3Export = new MockS3ExportImpl( "exportOneOrg.json" );
        //  s3Export.setFilename( "exportOneOrg.json" );
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );
        payload.put( "collectionName", "roles" );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );


        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        String collectionName = ( String ) collectionsMap.keySet().iterator().next();
        List collection = ( List ) collectionsMap.get( collectionName );

        for ( Object o : collection ) {
            Map entityMap = ( Map ) o;
            Map metadataMap = ( Map ) entityMap.get( "Metadata" );
            String entityName = ( String ) metadataMap.get( "name" );
            assertFalse( "junkRealName".equals( entityName ) );
        }
    }


    //
    //creation of files doesn't always delete itself
    @Test
    public void testExportOneAppOnCollectionEndpoint() throws Exception {

        final String orgName = uniqueOrg();
        final String appName = uniqueApp();


        File f = null;

        try {
            f = new File( "exportOneApp.json" );
        }
        catch ( Exception e ) {
            // consumed because this checks to see if the file exists.
            // If it doesn't, don't do anything and carry on.
        }
        f.deleteOnExit();


        Entity appInfo = setup.getEmf().createApplicationV2( orgName, appName );
        UUID applicationId = appInfo.getUuid();


        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        //intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[1];
        //creates entities
        for ( int i = 0; i < 1; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "junkRealName" );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "user", userProperties );
        }

        S3Export s3Export = new MockS3ExportImpl( "exportOneApp.json" );
        //s3Export.setFilename( "exportOneApp.json" );
        ExportService exportService = setup.getExportService();

        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        String collectionName = ( String ) collectionsMap.keySet().iterator().next();
        List collection = ( List ) collectionsMap.get( collectionName );

        for ( Object o : collection ) {
            Map entityMap = ( Map ) o;
            Map metadataMap = ( Map ) entityMap.get( "Metadata" );
            String entityName = ( String ) metadataMap.get( "name" );
            assertFalse( "junkRealName".equals( entityName ) );
        }
    }


    @Test
    public void testExportOneAppOnApplicationEndpointWQuery() throws Exception {

        File f = null;
        try {
            f = new File( "exportOneAppWQuery.json" );
        }
        catch ( Exception e ) {
            // consumed because this checks to see if the file exists.
            // If it doesn't, don't do anything and carry on.
        }
        f.deleteOnExit();


        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        //intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[1];
        //creates entities
        for ( int i = 0; i < 1; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "name", "me" );
            userProperties.put( "username", "junkRealName" );
            userProperties.put( "email", "burp" + i + "@anuff.com" );
            entity[i] = em.create( "users", userProperties );
        }

        S3Export s3Export = new MockS3ExportImpl( "exportOneAppWQuery.json" );
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "query", "select * where username = 'junkRealName'" );
        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        setup.getEntityIndex().refresh( applicationId );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        String collectionName = ( String ) collectionsMap.keySet().iterator().next();
        List collection = ( List ) collectionsMap.get( collectionName );

        for ( Object o : collection ) {
            Map entityMap = ( Map ) o;
            Map metadataMap = ( Map ) entityMap.get( "Metadata" );
            String entityName = ( String ) metadataMap.get( "name" );
            assertFalse( "junkRealName".equals( entityName ) );
        }
    }


    @Test
    public void testExportOneCollection() throws Exception {

        File f = null;
        int entitiesToCreate = 5;

        try {
            f = new File( "exportOneCollection.json" );
        }
        catch ( Exception e ) {
            // consumed because this checks to see if the file exists.
            // If it doesn't, don't do anything and carry on.
        }

        f.deleteOnExit();

        EntityManager em = setup.getEmf().getEntityManager( applicationId );

        // em.createApplicationCollection( "qtsMagics" );
        // intialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[entitiesToCreate];
        //creates entities
        for ( int i = 0; i < entitiesToCreate; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "qtsMagics", userProperties );
        }

        S3Export s3Export = new MockS3ExportImpl( "exportOneCollection.json" );
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );
        payload.put( "collectionName", "qtsMagics" );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        setup.getEntityIndex().refresh( applicationId );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        String collectionName = ( String ) collectionsMap.keySet().iterator().next();
        List collection = ( List ) collectionsMap.get( collectionName );

        assertEquals( entitiesToCreate, collection.size() );
    }


    @Test
    public void testExportOneCollectionWQuery() throws Exception {

        File f = null;
        int entitiesToCreate = 5;

        try {
            f = new File( "exportOneCollectionWQuery.json" );
        }
        catch ( Exception e ) {
            // consumed because this checks to see if the file exists.
            // If it doesn't, don't do anything and carry on.
        }
        f.deleteOnExit();

        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        em.createApplicationCollection( "baconators" );
        setup.getEntityIndex().refresh( applicationId );

        //initialize user object to be posted
        Map<String, Object> userProperties = null;
        Entity[] entity;
        entity = new Entity[entitiesToCreate];

        // creates entities
        for ( int i = 0; i < entitiesToCreate; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( "baconators", userProperties );
        }

        S3Export s3Export = new MockS3ExportImpl( "exportOneCollectionWQuery.json" );
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "query", "select * where username contains 'billybob0'" );
        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );
        payload.put( "collectionName", "baconators" );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        setup.getEntityIndex().refresh( applicationId );

        exportService.doExport( jobExecution );

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue( new FileReader( f ), typeRef );

        Map collectionsMap = ( Map ) jsonMap.get( "collections" );
        String collectionName = ( String ) collectionsMap.keySet().iterator().next();
        List collectionList = ( List ) collectionsMap.get( collectionName );

        assertEquals( 1, collectionList.size() );
    }


    @Test
    @Ignore( "this is a meaningless test because our export format does not support export of organizations" )
    public void testExportOneOrganization() throws Exception {

        // create a bunch of organizations, each with applications and collections of entities

        int maxOrgs = 3;
        int maxApps = 3;
        int maxEntities = 20;

        List<ApplicationInfo> appsMade = new ArrayList<>();
        List<OrganizationInfo> orgsMade = new ArrayList<>();

        for ( int orgIndex = 0; orgIndex < maxOrgs; orgIndex++ ) {


            String orgName = "org_" + RandomStringUtils.randomAlphanumeric( 10 );
            OrganizationInfo orgMade = setup.getMgmtSvc().createOrganization( orgName, adminUser, true );
            orgsMade.add( orgMade );
            logger.debug( "Created org {}", orgName );

            for ( int appIndex = 0; appIndex < maxApps; appIndex++ ) {

                String appName = "app_" + RandomStringUtils.randomAlphanumeric( 10 );
                ApplicationInfo appMade = setup.getMgmtSvc().createApplication( orgMade.getUuid(), appName );
                appsMade.add( appMade );
                logger.debug( "Created app {}", appName );

                for ( int entityIndex = 0; entityIndex < maxEntities; entityIndex++ ) {

                    EntityManager appEm = setup.getEmf().getEntityManager( appMade.getId() );
                    appEm.create( appName + "_type", new HashMap<String, Object>() {{
                        put( "property1", "value1" );
                        put( "property2", "value2" );
                    }} );
                }
            }
        }

        // export one of the organizations only, using mock S3 export that writes to local disk

        String exportFileName = "exportOneOrganization.json";
        S3Export s3Export = new MockS3ExportImpl( exportFileName );

        HashMap<String, Object> payload = payloadBuilder( appsMade.get( 0 ).getName() );
        payload.put( "organizationId", orgsMade.get( 0 ).getUuid() );
        payload.put( "applicationId", appsMade.get( 0 ).getId() );

        ExportService exportService = setup.getExportService();
        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );
        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        exportService.doExport( jobExecution );

        // finally, we check that file was created and contains the right number of entities in the array

        File exportedFile = new File( exportFileName );
        exportedFile.deleteOnExit();

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue( new FileReader( exportedFile ), typeRef );
        Map collectionsMap = ( Map ) jsonMap.get( "collections" );

        List collectionList = ( List ) collectionsMap.get( "users" );

        assertEquals( 3, collectionList.size() );
    }


    @Test
    public void testExportDoJob() throws Exception {

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );


        JobData jobData = new JobData();
        jobData.setProperty( "jobName", "exportJob" );

        // this needs to be populated with properties of export info
        jobData.setProperty( "exportInfo", payload );

        JobExecution jobExecution = mock( JobExecution.class );

        when( jobExecution.getJobData() ).thenReturn( jobData );
        when( jobExecution.getJobId() ).thenReturn( UUID.randomUUID() );

        ExportJob job = new ExportJob();
        ExportService eS = mock( ExportService.class );
        job.setExportService( eS );
        try {
            job.doJob( jobExecution );
        }
        catch ( Exception e ) {
            logger.error( "Error doing job", e );
            assert ( false );
        }
        assert ( true );
    }


    //tests that with empty job data, the export still runs.
    @Test
    public void testExportEmptyJobData() throws Exception {

        JobData jobData = new JobData();

        JobExecution jobExecution = mock( JobExecution.class );

        when( jobExecution.getJobData() ).thenReturn( jobData );
        when( jobExecution.getJobId() ).thenReturn( UUID.randomUUID() );

        ExportJob job = new ExportJob();
        S3Export s3Export = mock( S3Export.class );
        //setup.getExportService().setS3Export( s3Export );
        job.setExportService( setup.getExportService() );
        try {
            job.doJob( jobExecution );
        }
        catch ( Exception e ) {
            assert ( false );
        }
        assert ( true );
    }


    @Test
    public void testNullJobExecution() {

        JobData jobData = new JobData();

        JobExecution jobExecution = mock( JobExecution.class );

        when( jobExecution.getJobData() ).thenReturn( jobData );
        when( jobExecution.getJobId() ).thenReturn( UUID.randomUUID() );

        ExportJob job = new ExportJob();
        S3Export s3Export = mock( S3Export.class );
        // setup.getExportService().setS3Export( s3Export );
        job.setExportService( setup.getExportService() );
        try {
            job.doJob( jobExecution );
        }
        catch ( Exception e ) {
            assert ( false );
        }
        assert ( true );
    }


//    @Test
//    @Ignore // TODO: fix this test...
//    public void testIntegration100EntitiesOn() throws Exception {
//
//        logger.debug( "testIntegration100EntitiesOn(): starting..." );
//
//        ExportService exportService = setup.getExportService();
//
//        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
//        HashMap<String, Object> payload = payloadBuilder( appName );
//
//        payload.put( "organizationId", organization.getUuid() );
//        payload.put( "applicationId", applicationId );
//
//        // create five applications each with collection of five entities
//
//        for ( int i = 0; i < 5; i++ ) {
//
//            ApplicationInfo appMade = setup.getMgmtSvc().createApplication( organization.getUuid(), "superapp" + i );
//            EntityManager appEm = setup.getEmf().getEntityManager( appMade.getId() );
//
//            String collName = "superappCol" + i;
//            appEm.createApplicationCollection( collName );
//
//            Map<String, Object> entityLevelProperties = null;
//            Entity[] entNotCopied;
//            entNotCopied = new Entity[5];
//
//            for ( int index = 0; index < 5; index++ ) {
//                entityLevelProperties = new LinkedHashMap<String, Object>();
//                entityLevelProperties.put( "username", "bobso" + index );
//                entityLevelProperties.put( "email", "derp" + index + "@anuff.com" );
//                entNotCopied[index] = appEm.create( collName, entityLevelProperties );
//            }
//        }
//
//        // export the organization containing those apps and collections
//
//        UUID exportUUID = exportService.schedule( payload );
//
//        int maxRetries = 100;
//        int retries = 0;
//        while ( !exportService.getState( exportUUID ).equals( "FINISHED" ) && retries++ < maxRetries ) {
//            Thread.sleep( 100 );
//        }
//
//        String accessId = System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR );
//        String secretKey = System.getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR );
//        Properties overrides = new Properties();
//        overrides.setProperty( "s3" + ".identity", accessId );
//        overrides.setProperty( "s3" + ".credential", secretKey );
//
//        // test that we can find the file that were exported to S3
//
//        BlobStore blobStore = null;
//        try {
//
//            final Iterable<? extends Module> MODULES = ImmutableSet
//                .of( new JavaUrlHttpCommandExecutorServiceModule(), new Log4JLoggingModule(), new NettyPayloadModule() );
//
//            BlobStoreContext context =
//                ContextBuilder.newBuilder( "s3" ).credentials( accessId, secretKey ).modules( MODULES )
//                              .overrides( overrides ).buildView( BlobStoreContext.class );
//
//            String expectedFileName =
//                ( ( ExportServiceImpl ) exportService ).prepareOutputFileName( organization.getName(), "applications" );
//
//            blobStore = context.getBlobStore();
//            if ( !blobStore.blobExists( bucketName, expectedFileName ) ) {
//                blobStore.deleteContainer( bucketName );
//                Assert.fail( "Blob does not exist: " + expectedFileName );
//            }
//            Blob bo = blobStore.getBlob( bucketName, expectedFileName );
//
//            Long numOfFiles = blobStore.countBlobs( bucketName );
//            Long numWeWant = 1L;
//            blobStore.deleteContainer( bucketName );
//            assertEquals( numOfFiles, numWeWant );
//            assertNotNull( bo );
//        }
//        finally {
//            blobStore.deleteContainer( bucketName );
//        }
//    }


    //@Ignore("Why is this ignored?")
    @Test
    public void testIntegration100EntitiesForAllApps() throws Exception {

        S3Export s3Export = new AwsS3ExportImpl();
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        OrganizationInfo orgMade = null;
        ApplicationInfo appMade = null;
        for ( int i = 0; i < 5; i++ ) {
            orgMade = setup.getMgmtSvc().createOrganization( "minorboss" + i, adminUser, true );
            for ( int j = 0; j < 5; j++ ) {
                appMade = setup.getMgmtSvc().createApplication( orgMade.getUuid(), "superapp" + j );

                EntityManager customMaker = setup.getEmf().getEntityManager( appMade.getId() );
                customMaker.createApplicationCollection( "superappCol" + j );
                //intialize user object to be posted
                Map<String, Object> entityLevelProperties = null;
                Entity[] entNotCopied;
                entNotCopied = new Entity[1];
                //creates entities
                for ( int index = 0; index < 1; index++ ) {
                    entityLevelProperties = new LinkedHashMap<String, Object>();
                    entityLevelProperties.put( "derp", "bacon" );
                    entNotCopied[index] = customMaker.create( "superappCol" + j, entityLevelProperties );
                }
            }
        }

        payload.put( "organizationId", orgMade.getUuid() );

        //this kicks off the actual export
        UUID exportUUID = exportService.schedule( payload );
        assertNotNull( exportUUID );

        Thread.sleep( 3000 );

        String accessId = System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR );
        String secretKey = System.getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR );

        Properties overrides = new Properties();
        overrides.setProperty( "s3" + ".identity", accessId );
        overrides.setProperty( "s3" + ".credential", secretKey );

        //        BlobStore blobStore = null;
        //
        //        try {
        //            final Iterable<? extends Module> MODULES = ImmutableSet.of(
        //                new JavaUrlHttpCommandExecutorServiceModule(),
        //                new Log4JLoggingModule(),
        //                new NettyPayloadModule() );
        //
        ////            BlobStoreContext context = ContextBuilder.newBuilder( "s3" )
        ////                .credentials(accessId, secretKey )
        ////                .modules(MODULES )
        ////                .overrides(overrides )
        ////                .buildView(BlobStoreContext.class );
        //
        //            BlobStoreContext context = ContextBuilder.newBuilder( "aws-s3" )
        //                                                     .credentials( accessId, secretKey )
        //                                                     .modules( MODULES )
        //                                                     .buildView( BlobStoreContext.class );
        //
        //            blobStore = context.getBlobStore();
        //
        //            //Grab Number of files
        //            Long numOfFiles = blobStore.countBlobs( bucketName );
        //
        //            String expectedFileName = ((ExportServiceImpl)exportService)
        //                .prepareOutputFileName( organization.getName(), "applications" );
        //
        //            //delete container containing said files
        //            Blob bo = blobStore.getBlob( bucketName, expectedFileName );
        //            Long numWeWant = 5L;
        //            //blobStore.deleteContainer( bucketName );
        //
        //            //asserts that the correct number of files was transferred over
        //            assertEquals( numWeWant, numOfFiles );
        //
        //        }
        //        catch(Exception e){
        //            fail( e.getMessage() );
        //        }
        //        finally {
        //            //blobStore.deleteContainer( bucketName );
        //        }
    }


//    @Ignore( "Why is this ignored" )
//    @Test
//    public void testIntegration100EntitiesOnOneOrg() throws Exception {
//
//        S3Export s3Export = new AwsS3ExportImpl();
//        ExportService exportService = setup.getExportService();
//
//        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
//        HashMap<String, Object> payload = payloadBuilder( appName );
//
//        payload.put( "organizationId", organization.getUuid() );
//        payload.put( "applicationId", applicationId );
//
//        OrganizationInfo orgMade = null;
//        ApplicationInfo appMade = null;
//        for ( int i = 0; i < 100; i++ ) {
//            orgMade = setup.getMgmtSvc().createOrganization( "largerboss" + i, adminUser, true );
//            appMade = setup.getMgmtSvc().createApplication( orgMade.getUuid(), "superapp" + i );
//
//            EntityManager customMaker = setup.getEmf().getEntityManager( appMade.getId() );
//            customMaker.createApplicationCollection( "superappCol" + i );
//            //intialize user object to be posted
//            Map<String, Object> entityLevelProperties = null;
//            Entity[] entNotCopied;
//            entNotCopied = new Entity[20];
//            //creates entities
//            for ( int index = 0; index < 20; index++ ) {
//                entityLevelProperties = new LinkedHashMap<String, Object>();
//                entityLevelProperties.put( "username", "bobso" + index );
//                entityLevelProperties.put( "email", "derp" + index + "@anuff.com" );
//                entNotCopied[index] = customMaker.create( "superappCol", entityLevelProperties );
//            }
//        }
//
//        EntityManager em = setup.getEmf().getEntityManager( applicationId );
//
//        //intialize user object to be posted
//        Map<String, Object> userProperties = null;
//        Entity[] entity;
//        entity = new Entity[100];
//
//        //creates entities
//        for ( int i = 0; i < 100; i++ ) {
//            userProperties = new LinkedHashMap<String, Object>();
//            userProperties.put( "username", "bido" + i );
//            userProperties.put( "email", "bido" + i + "@anuff.com" );
//
//            entity[i] = em.create( "user", userProperties );
//        }
//
//        UUID exportUUID = exportService.schedule( payload );
//
//        while ( !exportService.getState( exportUUID ).equals( "FINISHED" ) ) {
//        }
//
//        String accessId = System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR );
//        String secretKey = System.getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR );
//
//        Properties overrides = new Properties();
//        overrides.setProperty( "s3" + ".identity", accessId );
//        overrides.setProperty( "s3" + ".credential", secretKey );
//
//        Blob bo = null;
//        BlobStore blobStore = null;
//
//        try {
//            final Iterable<? extends Module> MODULES = ImmutableSet
//                .of( new JavaUrlHttpCommandExecutorServiceModule(), new Log4JLoggingModule(),
//                    new NettyPayloadModule() );
//
//            BlobStoreContext context =
//                ContextBuilder.newBuilder( "s3" ).credentials( accessId, secretKey ).modules( MODULES )
//                              .overrides( overrides ).buildView( BlobStoreContext.class );
//
//            String expectedFileName =
//                ( ( ExportServiceImpl ) exportService ).prepareOutputFileName( organization.getName(), "applications" );
//
//            blobStore = context.getBlobStore();
//            if ( !blobStore.blobExists( bucketName, expectedFileName ) ) {
//                assert ( false );
//            }
//            Long numOfFiles = blobStore.countBlobs( bucketName );
//            Long numWeWant = Long.valueOf( 1 );
//            assertEquals( numOfFiles, numWeWant );
//
//            bo = blobStore.getBlob( bucketName, expectedFileName );
//        }
//        catch ( Exception e ) {
//            assert ( false );
//        }
//
//        assertNotNull( bo );
//        blobStore.deleteContainer( bucketName );
//    }


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
        Map<String, Object> properties = new HashMap<String, Object>();
        Map<String, Object> storage_info = new HashMap<String, Object>();
        storage_info.put( "s3_key", "null");
        storage_info.put( "s3_access_id", "null" );
        storage_info.put( "bucket_location", "null" );

        properties.put( "storage_provider", "s3" );
        properties.put( "storage_info", storage_info );

        payload.put( "path", orgOrAppName );
        payload.put( "properties", properties );
        return payload;
    }
}
