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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;

import org.apache.usergrid.NewOrgAppAdminRule;
import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.JobSchedulerService;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
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

import static org.apache.usergrid.TestHelper.newUUIDString;
import static org.junit.Assert.assertEquals;
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
        int numberOfEntitiesToBeWritten = 1000;
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

    @Test
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

        assertEquals( "There should have been two valid connections in the file", 2, validConnectionCounter );
    }


    @Test
    public void testExportOneOrgCollectionEndpoint() throws Exception {

        String testFileName = "exportOneOrg.json";

        //create another org to ensure we don't export it
        newOrgAppAdminRule.createOwnerAndOrganization( "noExport" + newUUIDString(), "junkUserName" + newUUIDString(),
            "junkRealName" + newUUIDString(), newUUIDString() + "ugExport@usergrid.com", "123456789" );

        S3Export s3Export = new MockS3ExportImpl( "exportOneOrg.json" );
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

        int validEntitiesCounter = 0;
        final InputStream in = new FileInputStream( "entities1"+testFileName );
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory(  ).createParser( in );


            Iterator jsonIterator = mapper.readValues( jp, typeRef);
            while(jsonIterator.hasNext()){
                Object jsonEntity =  jsonIterator.next();
                validEntitiesCounter++;
            }
        }finally{
            in.close();
            File exportedFile = new File("entities1"+testFileName);
            exportedFile.delete();
            exportedFile = new File("connections1"+testFileName);
            exportedFile.delete();
        }
        assertEquals( "There should have been three default roles in the file",3,validEntitiesCounter );

    }

    @Test
    public void testQueryAppliesToAllCollectionExport() throws Exception {

        String testFileName = "exportOneAppWQuery.json";

        //Create test entities.
        Entity[] firstCollectionEntities = createTestEntities( 10 );
        Entity[] secondCollectionEntities = createTestEntities( 10,"secondCollectionEntities" );


        S3Export s3Export = new MockS3ExportImpl( "exportOneAppWQuery.json" );
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        payload.put( "query", "select * where username = 'billybob0'" );
        payload.put( "organizationId", organization.getUuid() );
        payload.put( "applicationId", applicationId );

        UUID exportUUID = exportService.schedule( payload );

        JobData jobData = jobDataCreator( payload, exportUUID, s3Export );

        JobExecution jobExecution = mock( JobExecution.class );
        when( jobExecution.getJobData() ).thenReturn( jobData );

        setup.getEntityIndex().refresh( applicationId );

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
                //Could do further verification by checking to see if they have the correct collection name.
                if(( ( HashMap ) jsonEntity ).get( "uuid" ).equals( firstCollectionEntities[0].getUuid().toString())){
                    validEntitiesCounter++;
                }
                else if(( ( HashMap ) jsonEntity ).get( "uuid" ).equals( secondCollectionEntities[0].getUuid().toString())) {
                    validEntitiesCounter++;
                }
            }
        }finally{
            in.close();
            File exportedFile = new File("entities1"+testFileName);
            exportedFile.delete();
            exportedFile = new File("connections1"+testFileName);
            exportedFile.delete();
        }

        assertEquals( "There should have been one valid entity in the file",2,validEntitiesCounter );
    }


    /**
     * Creates entities that can easily be used to verify that export is working.
     * @param numberOfEntitiesCreated
     * @param collectionName
     * @return
     * @throws Exception
     */
    public Entity[] createTestEntities (int numberOfEntitiesCreated, String collectionName) throws Exception{
        EntityManager em = setup.getEmf().getEntityManager( applicationId );
        Entity[] entity = new Entity[numberOfEntitiesCreated];;
        Map<String, Object> userProperties = null;

        for ( int i = 0; i < numberOfEntitiesCreated; i++ ) {
            userProperties = new LinkedHashMap<String, Object>();
            userProperties.put( "username", "billybob" + i );
            userProperties.put( "email", "test" + i + "@anuff.com" );
            entity[i] = em.create( collectionName, userProperties );
        }

        return entity;
    }

    public Entity[] createTestEntities (int numberOfEntitiesCreated) throws Exception{
        return createTestEntities( numberOfEntitiesCreated, "baconrulesall" );
    }

    @Test
    public void testExportOneCollectionWQuery() throws Exception {

        String testFileName = "exportOneCollectionWQuery.json";
        int entitiesToCreate = 5;

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
            }
        }finally{
            in.close();
            File exportedFile = new File("entities1"+testFileName);
            exportedFile.delete();
            exportedFile = new File("connections1"+testFileName);
            exportedFile.delete();
        }

        assertEquals( "There should have been one valid entity in the file", 1, validEntitiesCounter );
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
