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


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.usergrid.NewOrgAppAdminRule;
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
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;


/**
 *
 *
 */
public class ExportServiceIT extends AbstractServiceIT {

    private static final Logger logger = LoggerFactory.getLogger( ExportServiceIT.class );

    @Rule
    public NoAWSCredsRule noCredsRule = new NoAWSCredsRule();

    @Rule
    public NewOrgAppAdminRule newOrgAppAdminRule = new NewOrgAppAdminRule( setup );

    // app-level data generated only once
    private UserInfo adminUser;
    private OrganizationInfo organization;
    private UUID applicationId;
    private AmazonS3 s3Client;
    boolean configured = false;


    private static String bucketPrefix;

    private String bucketName;


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

        configured =
            !StringUtils.isEmpty( System.getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR ) ) && !StringUtils
                .isEmpty( System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR ) ) && !StringUtils
                .isEmpty( System.getProperty( "bucket_location" ) );

        if ( !configured ) {
            logger.warn( "Skipping test because {}, {} and bucketName not "
                    + "specified as system properties, e.g. in your Maven settings.xml file.", new Object[] {
                    SDKGlobalConfiguration.SECRET_KEY_ENV_VAR, SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR
                } );
        }
        else {

            //  Assume.assumeTrue( configured );

            adminUser = newOrgAppAdminRule.getAdminInfo();
            organization = newOrgAppAdminRule.getOrganizationInfo();
            applicationId = newOrgAppAdminRule.getApplicationInfo().getId();
            bucketPrefix = System.getProperty( "bucket_location" );
            bucketName = bucketPrefix + RandomStringUtils.randomAlphabetic( 10 ).toLowerCase();
            //bucketName =  System.getProperty( "bucket_location" );

            AWSCredentials credentials =
                new BasicAWSCredentials( System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR ), System
                    .getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR ) );
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setProtocol( Protocol.HTTP );

            s3Client = new AmazonS3Client( credentials, clientConfig );
        }
    }


    //Deletes the files inside the bucket that was used for testing and then deletes the bucket.
    @After
    public void after() {
        if(configured == true) {
            try {
                //should probably refactor bucketname to be passed in, but then again for a test we'd only use one.
                Set<String> objectsInBucket = returnObjectListsFromBucket();

                for ( String objectInBucket : objectsInBucket ) {
                    DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest( bucketName, objectInBucket );
                    s3Client.deleteObject( deleteObjectRequest );
                }
            }
            catch ( MultiObjectDeleteException e ) {
                // Process exception.
                throw e;
            }
            s3Client.deleteBucket( bucketName );
        }
    }

    //What is required to make this test a reality?
    //Second we need a way to delete the file and bucket out of s3 once data has been verified.
    @Ignore("Doesn't work due to failure of aws creds rule.")
    @Test
    public void testConnectionsOnApplicationExport() throws Exception {
        //Populate a application with data that contains connections.
        ExportService exportService = setup.getExportService();

        ApplicationInfo appMade = setup.getMgmtSvc().getApplicationInfo( app.getOrgName() + "/" + app.getAppName().toLowerCase() );
        String orgName = setup.getMgmtSvc().getOrganizations().values().iterator().next();

        OrganizationInfo orgMade = setup.getMgmtSvc().getOrganizationByName( orgName );


        EntityManager em = setup.getEmf().getEntityManager( appMade.getId() );
        em.createApplicationCollection( "testConnectionsOnApplicationCol" );

        Entity[] entity;
        entity = new Entity[2];

        for ( int i = 0; i < 2; i++ ) {
            app.put( "username", "billybob" + RandomStringUtils.randomAlphabetic( 5 ).toLowerCase() );
            app.put( "email", "test" + RandomStringUtils.randomAlphabetic( 5 ).toLowerCase() + i + "@anuff.com" );

            entity[i] = app.testRequest( ServiceAction.POST, 1, "users" ).getEntity();
            app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[i] ).getUsername() );
        }

        //POST users/conn-user1/manages/user2/conn-user2
        app.testRequest( ServiceAction.POST, 1, "users", ( ( User ) entity[0] ).getUsername(), "manages", "users",
            ( ( User ) entity[1] ).getUsername() );
        app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[0] ).getUsername() );

        //POST users/conn-user1/reports/users/conn-user2
        app.testRequest( ServiceAction.POST, 1, "users", ( ( User ) entity[1] ).getUsername(), "relates", "users",
            ( ( User ) entity[0] ).getUsername() );
        Entity user2 =
            app.testRequest( ServiceAction.GET, 1, "users", ( ( User ) entity[1] ).getUsername() ).getEntity();

        setup.refreshIndex( appMade.getId() );

        HashMap<String, Object> payload = payloadBuilder( appMade.getName() );

        payload.put( "organizationId", orgMade.getUuid() );
        payload.put( "applicationId", appMade.getId() );

        //this kicks off the actual export with a unique bucket and filename.
        UUID exportUUID = exportService.schedule( payload );
        assertNotNull( exportUUID );

        //Anything around two seconds isn't enough for s3 so I bumped it up to 4 seconds to be safe.
        Thread.sleep( 4000 );

        logger.info( "Downloading an object" );

        Set<String> s3ObjectList = returnPrefixListFromBucket( "entities" );
        assertNotEquals( 0, s3ObjectList.size() );

        int validEntitiesCounter = 0;
        for ( String s3ObjectKey : s3ObjectList ) {

            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

            S3ObjectInputStream s3ObjectInputStream = s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();

            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory().createParser( s3ObjectInputStream );

            Iterator jsonIterator = mapper.readValues( jp, typeRef );
            while ( jsonIterator.hasNext() ) {
                Object jsonEntity = jsonIterator.next();
                if ( ( ( HashMap ) jsonEntity ).get( "uuid" ).equals( entity[0].getUuid().toString() ) )
                    validEntitiesCounter++;
                else if ( ( ( HashMap ) jsonEntity ).get( "uuid" ).equals( entity[1].getUuid().toString() ) )
                    validEntitiesCounter++;
            }
            assertEquals( "There should have been 2 valid entities in the file", 2, validEntitiesCounter );
        }
    }


    /**
     * Test that a single application gets exported despite there being many applications and
     * organizations with different applications.
     * @throws Exception
     */
    @Ignore("Doesn't work due to failure of aws creds rule.")
    @Test
    public void testApplicationExport() throws Exception {
        ExportService exportService = setup.getExportService();

        String appName = newOrgAppAdminRule.getApplicationInfo().getName();
        HashMap<String, Object> payload = payloadBuilder( appName );

        OrganizationInfo orgMade = null;
        ApplicationInfo appMade = null;
        Entity[] entity;
        entity = new Entity[5];
        for ( int i = 0; i < 5; i++ ) {
            orgMade = setup.getMgmtSvc().createOrganization( "minorboss" + i, adminUser, true );
            for ( int j = 0; j < 5; j++ ) {
                appMade = setup.getMgmtSvc().createApplication( orgMade.getUuid(), "superapp" + j );

                EntityManager customMaker = setup.getEmf().getEntityManager( appMade.getId() );
                customMaker.createApplicationCollection( "superappCol" + j );
                //intialize user object to be posted
                Map<String, Object> entityLevelProperties = null;
                //creates entities
                for ( int index = 0; index < 5; index++ ) {
                    entityLevelProperties = new LinkedHashMap<String, Object>();
                    entityLevelProperties.put( "derp", "bacon" );
                    entity[index] = customMaker.create( "superappCol" + j, entityLevelProperties );
                }
            }
        }

        payload.put( "organizationId", orgMade.getUuid() );

        //this kicks off the actual export with a unique bucket and filename.
        UUID exportUUID = exportService.schedule( payload );
        assertNotNull( exportUUID );

        //Anything around two seconds isn't enough for s3 so I bumped it up to 5 seconds to be safe.
        Thread.sleep( 5000 );

        logger.info( "Downloading an object" );

        Set<String> s3ObjectList = returnPrefixListFromBucket( "entities" );
        assertNotEquals( 0, s3ObjectList.size() );

        int validEntitiesCounter = 0;
        for ( String s3ObjectKey : s3ObjectList ) {

            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

            S3ObjectInputStream s3ObjectInputStream = s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();

            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = new JsonFactory().createParser( s3ObjectInputStream );

            Iterator jsonIterator = mapper.readValues( jp, typeRef );
            while ( jsonIterator.hasNext() ) {
                Object jsonEntity = jsonIterator.next();
                if(( ( HashMap ) jsonEntity ).get( "derp" )!=null)
                    if ( ( ( HashMap ) jsonEntity ).get( "derp" ).equals( "bacon" ))
                        validEntitiesCounter++;

            }
        }
        assertEquals( "There should have been 25 valid entities in the file", 25, validEntitiesCounter );

    }

    @Ignore("Doesn't work due to failure of aws creds rule.")
    @Test
    public void testConnectionsOnApplicationEndpoint() throws Exception {

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
        //this kicks off the actual export with a unique bucket and filename.
        UUID exportUUID = exportService.schedule( payload );
        assertNotNull( exportUUID );
        Thread.sleep( 5000 );

        Set<String> s3EntityFileList = returnPrefixListFromBucket( "entities" );
        assertNotEquals( 0, s3EntityFileList.size() );

        Set<String> s3ConnectionFileList = returnPrefixListFromBucket( "connections" );
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

        int validEntitiesCounter = 0;
        for(String s3ObjectKey: s3EntityFileList) {
            try {
                S3ObjectInputStream s3ObjectInputStream =
                    s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();
                ObjectMapper mapper = new ObjectMapper();
                JsonParser jp = new JsonFactory().createParser( s3ObjectInputStream );

                Iterator jsonIterator = mapper.readValues( jp, typeRef );
                while ( jsonIterator.hasNext() ) {
                    Object jsonEntity = jsonIterator.next();
                    if ( ( ( HashMap ) jsonEntity ).get( "uuid" ).equals( entity[0].getUuid().toString() ) ) {
                        validEntitiesCounter++;
                    }
                    else if ( ( ( HashMap ) jsonEntity ).get( "uuid" ).equals( entity[1].getUuid().toString() ) ) {
                        validEntitiesCounter++;
                    }
                }
            }catch ( Exception e ){
                throw new Exception( e );
            }
        }

        assertEquals( "There should have been two valid entities in the file", 2, validEntitiesCounter );

        //Read the connection entities and verify that they are correct

        int validConnectionCounter = 0;
        for(String s3ObjectKey : s3ConnectionFileList) {
            try {
                S3ObjectInputStream s3ObjectInputStream = s3Client.getObject( bucketName, s3ObjectKey ).getObjectContent();
                ObjectMapper mapper = new ObjectMapper();
                JsonParser jp = new JsonFactory().createParser( s3ObjectInputStream );

                Iterator jsonIterator = mapper.readValues( jp, typeRef );
                while ( jsonIterator.hasNext() ) {
                    Object jsonConnection = jsonIterator.next();
                    if ( ( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() ) != null ) {
                        Object jsonEntityConnection = ( ( HashMap ) jsonConnection ).get( entity[1].getUuid().toString() );
                        assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );

                        //String is stored in an array so we need to trim array fixings off the end of the string to compare.
                        String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
                        connectionUuid = connectionUuid.substring( 1, connectionUuid.length() - 1 );
                        if ( connectionUuid.equals( entity[0].getUuid().toString() ) ) validConnectionCounter++;
                    }
                    else if ( ( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString() ) != null ) {
                        Object jsonEntityConnection =
                            ( ( HashMap ) jsonConnection ).get( entity[0].getUuid().toString() );
                        assertNotNull( ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ) );

                        String connectionUuid = ( ( HashMap ) jsonEntityConnection ).get( "vibrations" ).toString();
                        connectionUuid = connectionUuid.substring( 1, connectionUuid.length() - 1 );

                        if ( connectionUuid.equals( entity[1].getUuid().toString() ) ) validConnectionCounter++;
                    }
                }
            }catch(Exception e){
                throw new Exception( e );
            }
        }
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
        storage_info.put( "s3_key", System.getProperty( SDKGlobalConfiguration.SECRET_KEY_ENV_VAR ) );
        storage_info.put( "s3_access_id", System.getProperty( SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR ) );
        storage_info.put( "bucket_location", bucketName );

        properties.put( "storage_provider", "s3" );
        properties.put( "storage_info", storage_info );

        payload.put( "path", orgOrAppName );
        payload.put( "properties", properties );
        return payload;
    }


    /**
     * Retrieves all files from specified bucketname
     * @return
     */
    public Set<String> returnObjectListsFromBucket() {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName( bucketName );
        ObjectListing objectListing;
        Set<String> bucketObjectNames = new HashSet<>();

        objectListing = s3Client.listObjects( listObjectsRequest );
        for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ) {
            System.out.println( " - " + objectSummary.getKey() + "  " +
                "(size = " + objectSummary.getSize() +
                ")" );
            bucketObjectNames.add( objectSummary.getKey() );
        }
        return bucketObjectNames;
    }

    /**
     * returns filelist with specified prefix.
     * @return
     */
    public Set<String> returnPrefixListFromBucket(String bucketPrefix) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName( bucketName );
        ObjectListing objectListing;
        Set<String> bucketObjectNames = new HashSet<>();

        objectListing = s3Client.listObjects( listObjectsRequest );
        for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ) {
            System.out.println( " - " + objectSummary.getKey() + "  " +
                "(size = " + objectSummary.getSize() +
                ")" );
            if(objectSummary.getKey().contains( bucketPrefix ))
                bucketObjectNames.add( objectSummary.getKey() );
        }
        return bucketObjectNames;
    }
}
