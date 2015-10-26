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


import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.management.RuntimeErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.SchedulerService;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityManagerFactory;
import org.apache.usergrid.persistence.PagingResultsIterator;
import org.apache.usergrid.persistence.Query;
import org.apache.usergrid.persistence.Query.Level;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.entities.Export;
import org.apache.usergrid.persistence.entities.JobData;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;


/**
 * Need to refactor out the mutliple orgs being take , need to factor out the multiple apps it will just be the one app
 * and the one org and all of it's collections.
 */
public class ExportServiceImpl implements ExportService {


    private static final Logger logger = LoggerFactory.getLogger( ExportServiceImpl.class );
    public static final String EXPORT_ID = "exportId";
    public static final String EXPORT_JOB_NAME = "exportJob";
    //Injected scheduler service to run the job through the scheduler.
    private SchedulerService sch;

    //injected the Entity Manager Factory to access entity manager
    protected EntityManagerFactory emf;

    //EntityManager that will only be used for the management application and updating export entities
    protected EntityManager em;


    //inject Management Service to access Organization Data
    private ManagementService managementService;

    //inject service manager to get connections

    //Maximum amount of entities retrieved in a single go.
    public static final int MAX_ENTITY_FETCH = 250;

    //Amount of time that has passed before sending another heart beat in millis
    public static final int TIMESTAMP_DELTA = 5000;

    private JsonFactory jsonFactory = new JsonFactory();

    public ExportServiceImpl(){
        em = emf.getEntityManager( emf.getManagementAppId() );
    }


    @Override
    public UUID schedule( final Map<String, Object> config ) throws Exception {
        logger.debug( "Starting to schedule the export job" );

        if ( config == null ) {
            logger.error( "export information cannot be null" );
            return null;
        }

        EntityManager em = null;
        try {
            em = emf.getEntityManager( emf.getManagementAppId() );
            Set<String> collections = em.getApplicationCollections();

            if ( !collections.contains( "exports" ) ) {
                em.createApplicationCollection( "exports" );
            }
        }
        catch ( Exception e ) {
            logger.error( "application doesn't exist within the current context" );
            return null;
        }

        Export export = new Export();

        //update state
        try {
            export = em.create( export );
        }
        catch ( Exception e ) {
            logger.error( "Export entity creation failed" );
            return null;
        }

        export.setState( Export.State.CREATED );
        em.update( export );
        //set data to be transferred to exportInfo
        JobData jobData = new JobData();
        jobData.setProperty( "exportInfo", config );
        jobData.setProperty( EXPORT_ID, export.getUuid() );

        long soonestPossible = System.currentTimeMillis() + 250; //sch grace period

        //schedule job
        logger.debug( "Creating the export job with the name: "+ EXPORT_JOB_NAME );
        sch.createJob( EXPORT_JOB_NAME, soonestPossible, jobData );

        //update state
        export.setState( Export.State.SCHEDULED );
        em.update( export );

        return export.getUuid();
    }

    @Override
    public UUID schedule( final Map<String, Object> config, final ExportFilter exportFilter ) throws Exception {
        logger.debug( "Starting to schedule the export job" );

        if ( config == null ) {
            logger.error( "export information cannot be null" );
            return null;
        }

        EntityManager em = null;
        try {
            em = emf.getEntityManager( emf.getManagementAppId() );
            Set<String> collections = em.getApplicationCollections();

            if ( !collections.contains( "exports" ) ) {
                em.createApplicationCollection( "exports" );
            }
        }
        catch ( Exception e ) {
            logger.error( "application doesn't exist within the current context" );
            return null;
        }

        Export export = new Export();

        //update state
        try {
            export = em.create( export );
        }
        catch ( Exception e ) {
            logger.error( "Export entity creation failed" );
            return null;
        }

        export.setState( Export.State.CREATED );
        em.update( export );
        //set data to be transferred to exportInfo
        JobData jobData = new JobData();
        jobData.setProperty( "target", config );
        jobData.setProperty( "filters", exportFilter);
        jobData.setProperty( EXPORT_ID, export.getUuid() );


        long soonestPossible = System.currentTimeMillis() + 250; //sch grace period

        //schedule job
        logger.debug( "Creating the export job with the name: "+ EXPORT_JOB_NAME );
        sch.createJob( EXPORT_JOB_NAME, soonestPossible, jobData );

        //update state
        updateExportStatus( export, Export.State.SCHEDULED,null );

        return export.getUuid();
    }


    /**
     * Query Entity Manager for the string state of the Export Entity. This corresponds to the GET /export
     *
     * @return String
     */
    @Override
    public String getState( final UUID uuid ) throws Exception {

        if ( uuid == null ) {
            logger.error( "UUID passed in cannot be null." );
            return "UUID passed in cannot be null";
        }

        EntityManager rootEm = emf.getEntityManager( emf.getManagementAppId() );

        logger.debug( "Searching for export entity with the following uuid: "+ uuid.toString() );
        //retrieve the export entity.
        Export export = rootEm.get( uuid, Export.class );

        if ( export == null ) {
            logger.error( "no entity with that uuid was found" );
            return "No Such Element found";
        }
        return export.getState().toString();
    }


    @Override
    public String getErrorMessage( final UUID appId, final UUID uuid ) throws Exception {

        //get application entity manager
        if ( appId == null ) {
            logger.error( "Application context cannot be found." );
            return "Application context cannot be found.";
        }

        if ( uuid == null ) {
            logger.error( "UUID passed in cannot be null." );
            return "UUID passed in cannot be null";
        }

        EntityManager rootEm = emf.getEntityManager( appId );

        //retrieve the export entity.
        Export export = rootEm.get( uuid, Export.class );

        if ( export == null ) {
            logger.error( "no entity with that uuid was found" );
            return "No Such Element found";
        }
        return export.getErrorMessage();
    }


    //This flow that is detailed
    //The responsibilities of this method is to error check the configuration that is passed to us in job execution
    //Then it also delegates to the correct type of export.

    //Seperate into two methods, one for error checking and the other for checking the filters?

    //what should this method do? It should handle the flow of export. Aka by looking at this method we should be able to see
    //the different steps that we need to take in order to do a export.

    //Extract the job data
    //Determine
    @Override
    public void doExport( final JobExecution jobExecution ) throws Exception {

        final JobData jobData = jobExecution.getJobData();

        Map<String, Object> config = ( Map<String, Object> ) jobData.getProperty( "exportInfo" );
        if ( config == null ) {
            logger.error( "Export Information passed through is null" );
            return;
        }

        UUID exportId = ( UUID ) jobData.getProperty( EXPORT_ID );

        //TODO:GREY doesn't need to get referenced everytime. Should only be set once and then used everywhere.
      //  EntityManager em = emf.getEntityManager( emf.getManagementAppId() );
        Export export = em.get( exportId, Export.class );

        //update the entity state to show that the job has officially started.
        logger.debug( "Starting export job with uuid: "+export.getUuid() );
        export.setState( Export.State.STARTED );
        em.update( export );

        //Checks to see if the job was given a different s3 export class. ( Local or Aws )
        S3Export s3Export = null;
        try {
            s3Export = s3ExportDeterminator( jobData );
        }catch(Exception e) {
            updateExportStatus( export, Export.State.FAILED, e.getMessage() );
            throw e;
        }

        //This is defensive programming against anybody who wants to run the export job.
        //They need to add the organization id or else we won't know where the job came from or what it has
        //access to.
        if ( config.get( "organizationId" ) == null ) {
            logger.error( "No organization uuid was associated with this call. Exiting." );
            updateExportStatus( export, Export.State.FAILED,"No organization could be found" );
            return;
        }

        //extracts the filter information
        ExportFilter exportFilter = parseFilterInformation(jobData);

        //Start the beginning of the flow.
        exportApplicationsFromOrg( (UUID)config.get( "organizationId" ),config,jobExecution,s3Export,exportFilter );

        logger.debug( "finished the export job." );
        updateExportStatus( export,Export.State.FINISHED,null );
    }


    public SchedulerService getSch() {
        return sch;
    }


    public void setSch( final SchedulerService sch ) {
        this.sch = sch;
    }


    public EntityManagerFactory getEmf() {
        return emf;
    }


    public void setEmf( final EntityManagerFactory emf ) {
        this.emf = emf;
    }


    public ManagementService getManagementService() {

        return managementService;
    }


    public void setManagementService( final ManagementService managementService ) {
        this.managementService = managementService;
    }


    public Export getExportEntity( final JobExecution jobExecution ) throws Exception {

        UUID exportId = ( UUID ) jobExecution.getJobData().getProperty( EXPORT_ID );
        EntityManager exportManager = emf.getEntityManager( emf.getManagementAppId() );

        return exportManager.get( exportId, Export.class );
    }


    /**
     * Exports filtered applications or all of the applications.
     */
    private void exportApplicationsFromOrg( UUID organizationUUID, final Map<String, Object> config,
                                            final JobExecution jobExecution, S3Export s3Export, ExportFilter exportFilter ) throws Exception {

        //retrieves export entity
        Export export = getExportEntity( jobExecution );
        String appFileName = null;


        //if the application filter is empty then we need to export all of the applications.
        if(exportFilter.getApplications() == null || exportFilter.getApplications().isEmpty()) {

            BiMap<UUID, String> applications = managementService.getApplicationsForOrganization( organizationUUID );

            for ( Map.Entry<UUID, String> application : applications.entrySet() ) {

                if ( application.getValue().equals(
                    managementService.getOrganizationByUuid( organizationUUID ).getName() + "/exports" ) ) {
                    continue;
                }

                appFileName = prepareOutputFileName( application.getValue(), null );

                Map ephemeral = collectionExportAndQuery( application.getKey(), config, export, jobExecution,exportFilter );

                fileTransfer( export, appFileName, ephemeral, config, s3Export );
            }
        }
        //export filter lists specific applications that need to be exported.
        else{
            BiMap<String,UUID> applications = managementService.getApplicationsForOrganization( organizationUUID ).inverse();
            Set<String> applicationSet = exportFilter.getApplications();


            for(String applicationName : applicationSet){
                appFileName = prepareOutputFileName( applicationName, null );

                Map ephemeral = collectionExportAndQuery(applications.get( applicationName ), config, export, jobExecution,exportFilter );

                fileTransfer( export, appFileName, ephemeral, config, s3Export );
            }
        }
    }


    public void fileTransfer( Export export, String appFileName, Map ephemeral, Map<String, Object> config,
                              S3Export s3Export ) {
        try {
            s3Export.copyToS3( ephemeral, config, appFileName );

        }
        catch ( Exception e ) {
            logger.error( e.getMessage(),e );
            export.setErrorMessage( e.getMessage() );
            export.setState( Export.State.FAILED );
            return;
        }
    }

    /**
     * Regulates how long to wait until the next heartbeat.
     */
    public long checkTimeDelta( long startingTime, final JobExecution jobExecution ) {

        long cur_time = System.currentTimeMillis();

        if ( startingTime <= ( cur_time - TIMESTAMP_DELTA ) ) {
            jobExecution.heartbeat();
            return cur_time;
        }
        return startingTime;
    }


    /**
     * Serialize and save the collection members of this <code>entity</code>
     *
     * @param em Entity Manager
     * @param collection Collection Name
     * @param entity entity
     */
    private void saveCollectionMembers( JsonGenerator jg, EntityManager em, String collection, Entity entity )
            throws Exception {

        // Write dictionaries
        saveDictionaries( entity, em, jg );

        Set<String> collections = em.getCollections( entity );

        // If your application doesn't have any e
        if ( ( collections == null ) || collections.isEmpty() ) {
            return;
        }

        for ( String collectionName : collections ) {

            if ( collectionName.equals( collection ) ) {
                jg.writeFieldName( collectionName );
                jg.writeStartArray();

                //is 100000 an arbitary number?
                Results collectionMembers =
                        em.getCollection( entity, collectionName, null, 100000, Level.IDS, false );

                List<UUID> entityIds = collectionMembers.getIds();

                if ( ( entityIds != null ) && !entityIds.isEmpty() ) {
                    for ( UUID childEntityUUID : entityIds ) {
                        jg.writeObject( childEntityUUID.toString() );
                    }
                }

                // End collection array.
                jg.writeEndArray();
            }
        }
    }

    /**
    Returns dictionaries that contain values that should be exported. If this set contains 0 strings then that means
    there are no meaningful dictionaries to write and they shouldn't be exported or written to the exported file.

     In the best case we don't find any hits then we short circut the entire dictionary flow. That gives us O(n) perf.
     In the worst case the valid hit is at the end and we take a O(2n) search.

     One way this could be improved is to save all the things that are worth writing and then pass the set back
     with those values. Then if it is empty we still have O(n) but otherwise we just do O ( n+ values that need to be written ).
     //TODO: find a way to speed up dictionary exporting.

     */
    private Boolean worthExportingDictionaryValues(Entity entity, EntityManager em) throws Exception {
        //Retrieve all dictionaries that the entity has a reference to
        Set<String> dictionaries = em.getDictionaries( entity );

        for ( String dictionary : dictionaries ) {

            Map<Object, Object> dict = em.getDictionaryAsMap( entity, dictionary );

            // nothing to do
            if ( dict.isEmpty() ) {
                continue;
            }
            //yes it is worth returning the values
            return true;

        }
        //not it is not worth returning the values.
        return false;

    }

    /**
     * Persists the connection for this entity.
     */
    private void saveDictionaries( Entity entity, EntityManager em, JsonGenerator jg ) throws Exception {

        //Short circut if you don't find any dictionaries.
        Set<String> dictionaries = em.getDictionaries( entity );
        if(!worthExportingDictionaryValues(entity,em))
            return;

        jg.writeStartObject();
        jg.writeFieldName( "dictionaries" );
        jg.writeStartObject();

        for ( String dictionary : dictionaries ) {

            Map<Object, Object> dict = em.getDictionaryAsMap( entity, dictionary );

            // nothing to do
            if ( dict.isEmpty() ) {
                continue;
            }

            jg.writeFieldName( dictionary );

            jg.writeStartObject();

            for ( Map.Entry<Object, Object> entry : dict.entrySet() ) {
                jg.writeFieldName( entry.getKey().toString() );
                jg.writeObject( entry.getValue() );
            }

            jg.writeEndObject();
        }
        jg.writeEndObject();
        jg.writeEndObject();
    }


    /**
     * Persists the connection for this entity.
     */
    private void saveConnections( Entity entity, EntityManager em, JsonGenerator jg, ExportFilter exportFilter ) throws Exception {

        //Short circut if you don't find any connections.
        Set<String> connectionTypes = em.getConnectionsAsSource( entity );
        if(connectionTypes.size() == 0){
            logger.debug( "No connections found for this entity. uuid : "+entity.getUuid().toString());
            return;
        }

        //filtering gets applied here.
        if(exportFilter.getConnections() == null || exportFilter.getConnections().isEmpty()) {

            jg.writeStartObject();
            jg.writeFieldName( entity.getUuid().toString() );
            jg.writeStartObject();

            for ( String connectionType : connectionTypes ) {

                jg.writeFieldName( connectionType );
                jg.writeStartArray();

                Results results = em.getTargetEntities( entity, connectionType, null, Level.ALL_PROPERTIES );
                PagingResultsIterator connectionResults = new PagingResultsIterator( results );
                for ( Object c : connectionResults ) {
                    Entity connectionRef = ( Entity ) c;
                    jg.writeObject( connectionRef.getUuid() );
                }

                jg.writeEndArray();
            }
        }
        //filter connections
        else {
            Set<String> connectionSet = exportFilter.getConnections();
            for ( String connectionType : connectionTypes ) {
                if(connectionSet.contains( connectionType )) {
                    jg.writeFieldName( connectionType );
                    jg.writeStartArray();

                    Results results = em.getTargetEntities( entity, connectionType, null, Level.ALL_PROPERTIES );
                    PagingResultsIterator connectionResults = new PagingResultsIterator( results );
                    for ( Object c : connectionResults ) {
                        Entity connectionRef = ( Entity ) c;
                        jg.writeObject( connectionRef.getUuid() );
                    }

                    jg.writeEndArray();
                }
            }
        }
        jg.writeEndObject();
        jg.writeEndObject();
        jg.writeRaw( '\n' );
        jg.flush();
    }


    protected JsonGenerator getJsonGenerator( OutputStream entityOutputStream ) throws IOException {
        //TODO:shouldn't the below be UTF-16?

        //most systems are little endian.
        JsonGenerator jg = jsonFactory.createGenerator( entityOutputStream, JsonEncoding.UTF16_LE );
        jg.setPrettyPrinter( new MinimalPrettyPrinter( "" ) );
        jg.setCodec( new ObjectMapper() );
        return jg;
    }

    /**
     * @return the file name concatenated with the type and the name of the collection
     */
    public String prepareOutputFileName( String applicationName, String CollectionName ) {
        StringBuilder str = new StringBuilder();
        str.append( applicationName );
        str.append( "." );
        if ( CollectionName != null ) {
            str.append( CollectionName );
            str.append( "." );
        }
        str.append( System.currentTimeMillis() );
        str.append( ".json" );

        String outputFileName = str.toString();

        return outputFileName;
    }

//TODO: GREY need to create a method that will be called with a filename string then generate a output stream.
    //That output stream will then be fed into the json generator. and added to the entitiesToExport array.

    /**
     * handles the query and export of collections
     */
    //TODO:Needs further refactoring.
    protected Map collectionExportAndQuery( UUID applicationUUID, final Map<String, Object> config, Export export,
                                             final JobExecution jobExecution,ExportFilter exportFilter ) throws Exception {

        EntityManager em = emf.getEntityManager( applicationUUID );
        long starting_time = System.currentTimeMillis();
        //The counter needs to be constant across collections since application exports do across collection aggregation.
        int entitiesExportedCount = 0;


        //TODO:GREY Add config to change path where this file is written.

        List<File> entitiesToExport = new ArrayList<>(  );
        File entityFileToBeExported = new File( "tempEntityExportPart1");
        entityFileToBeExported.deleteOnExit();
        FileOutputStream fileEntityOutputStream = new FileOutputStream( entityFileToBeExported );
        OutputStream entityOutputStream = new BufferedOutputStream( fileEntityOutputStream );
        entitiesToExport.add( entityFileToBeExported );

        JsonGenerator jg = getJsonGenerator( entityOutputStream );

        //While this is more wordy it allows great seperation.
        List<File> connectionsToExport = new ArrayList<>(  );
        File connectionFileToBeExported = new File ("tempConnectionExportPart1");
        connectionFileToBeExported.deleteOnExit();
        FileOutputStream fileConnectionOutputStream = new FileOutputStream( connectionFileToBeExported );
        OutputStream connectionOutputStream = new BufferedOutputStream( fileConnectionOutputStream );
        connectionsToExport.add( connectionFileToBeExported );

        JsonGenerator connectionJsonGeneration = getJsonGenerator( connectionOutputStream );


        if(exportFilter.getCollections() == null || exportFilter.getCollections().isEmpty()) {
            Map<String, Object> metadata = em.getApplicationCollectionMetadata();
            for ( String collectionName : metadata.keySet() ) {


                //if the collection you are looping through doesn't match the name of the one you want. Don't export it.
                if ( ( config.get( "collectionName" ) == null ) || collectionName
                    .equalsIgnoreCase( ( String ) config.get( "collectionName" ) ) ) {

                    //Query entity manager for the entities in a collection
                    Query query = null;
                    if(exportFilter.getQuery() == null ) {
                        query = new Query();
                    }
                    else {
                        try {
                            query = exportFilter.getQuery();
                        }
                        catch ( Exception e ) {
                            export.setErrorMessage( e.getMessage() );
                        }
                    }
                    query.setLimit( MAX_ENTITY_FETCH );
                    query.setResultsLevel( Level.ALL_PROPERTIES );
                    query.setCollection( collectionName );

                    //counter that will inform when we should split into another file.
                    Results entities = em.searchCollection( em.getApplicationRef(), collectionName, query );

                    PagingResultsIterator itr = new PagingResultsIterator( entities );
                    int currentFilePartIndex = 1;

                    for ( Object e : itr ) {
                        starting_time = checkTimeDelta( starting_time, jobExecution );
                        Entity entity = ( Entity ) e;
                        jg.writeObject( entity );
                        saveCollectionMembers( jg, em, ( String ) config.get( "collectionName" ), entity );
                        saveConnections( entity, em, connectionJsonGeneration,exportFilter );
                        jg.writeRaw( '\n' );
                        jg.flush();
                        entitiesExportedCount++;
                        if ( entitiesExportedCount % 1000 == 0 ) {
                            //Time to split files
                            currentFilePartIndex++;
                            File entityFileToBeExported2 = new File( "tempEntityExportPart" + currentFilePartIndex );
                            //TODO: UNCOMMENT THIS OR THE JSON GENERATION WON'T WORK.
                            //jg = getJsonGenerator( entityFileToBeExported2 );
                            entityFileToBeExported2.deleteOnExit();
                            entitiesToExport.add( entityFileToBeExported2 );

                            //It is quite likely that there are files that do not contain any connections and thus there will not

                            //be anything to write to these empty connection files. Not entirely sure what to do
                            // about that at this point.
                            connectionFileToBeExported = new File( "tempConnectionExportPart" + currentFilePartIndex );
                            connectionFileToBeExported.deleteOnExit();
                            //connectionJsonGeneration = getJsonGenerator( connectionFileToBeExported );
                            connectionsToExport.add( connectionFileToBeExported );
                        }
                    }
                }
            }
        }
        //handles the case where there are specific collections that need to be exported.
        else{
            Set<String> collectionSet = exportFilter.getCollections();

            //loop through only the collection names
            for( String collectionName : collectionSet ) {
                //Query entity manager for the entities in a collection
                Query query = null;
                if(exportFilter.getQuery() == null ) {
                    query = new Query();
                }
                else {
                    try {
                        query = exportFilter.getQuery();
                    }
                    catch ( Exception e ) {
                        export.setErrorMessage( e.getMessage() );
                    }
                }
                query.setLimit( MAX_ENTITY_FETCH );
                query.setResultsLevel( Level.ALL_PROPERTIES );
                query.setCollection( collectionName );

                Results entities = em.searchCollection( em.getApplicationRef(), collectionName, query );

                PagingResultsIterator itr = new PagingResultsIterator( entities );
                //counter that will inform when we should split into another file.
                int currentFilePartIndex = 1;

                for ( Object e : itr ) {
                    starting_time = checkTimeDelta( starting_time, jobExecution );
                    Entity entity = ( Entity ) e;
                    jg.writeObject( entity );
                    saveCollectionMembers( jg, em, collectionName, entity );
                    saveConnections( entity, em, connectionJsonGeneration,exportFilter );
                    jg.writeRaw( '\n' );
                    jg.flush();
                    entitiesExportedCount++;
                    if ( entitiesExportedCount % 1000 == 0 ) {
                        //Time to split files
                        currentFilePartIndex++;
                        entityFileToBeExported = new File( "tempEntityExportPart" + currentFilePartIndex );
                        entityFileToBeExported.deleteOnExit();
                        fileEntityOutputStream = new FileOutputStream( entityFileToBeExported );
                        entityOutputStream = new BufferedOutputStream( fileEntityOutputStream );
                        entitiesToExport.add( entityFileToBeExported );

                        jg = getJsonGenerator( entityOutputStream );


                        //It is quite likely that there are files that do not contain any connections and thus there will not


                        //be anything to write to these empty connection files. Not entirely sure what to do
                        // about that at this point.
                        connectionFileToBeExported = new File( "tempConnectionExportPart" + currentFilePartIndex );
                        connectionFileToBeExported.deleteOnExit();
                        fileConnectionOutputStream = new FileOutputStream( connectionFileToBeExported );
                        connectionOutputStream = new BufferedOutputStream( fileConnectionOutputStream );
                        connectionJsonGeneration = getJsonGenerator( connectionOutputStream );
                        connectionsToExport.add( connectionFileToBeExported );
                    }
                }
            }
        }
        jg.flush();
        jg.close();

        HashMap<String,List> filePointers = new HashMap<>(  );
        filePointers.put( "entities",entitiesToExport );
        filePointers.put( "connections",connectionsToExport );

        return filePointers;
    }

    //TODO: GREY need to create a method that will be called with a filename string then generate a output stream.
    //That output stream will then be fed into the json generator. and added to the entitiesToExport array.

    /**
     * handles the query and export of collections
     */
    //TODO:Needs further refactoring.
    protected Map collectionExportAndQuery2( UUID applicationUUID, final Map<String, Object> config, Export export,
                                            final JobExecution jobExecution,ExportFilter exportFilter ) throws Exception {

        EntityManager em = emf.getEntityManager( applicationUUID );
        long starting_time = System.currentTimeMillis();
        //The counter needs to be constant across collections since application exports do across collection aggregation.
        int entitiesExportedCount = 0;


        //TODO:GREY Add config to change path where this file is written.

        List<File> entitiesToExport = new ArrayList<>(  );
        File entityFileToBeExported = new File( "tempEntityExportPart1");
        entityFileToBeExported.deleteOnExit();
        FileOutputStream fileEntityOutputStream = new FileOutputStream( entityFileToBeExported );
        OutputStream entityOutputStream = new BufferedOutputStream( fileEntityOutputStream );
        entitiesToExport.add( entityFileToBeExported );

        JsonGenerator jg = getJsonGenerator( entityOutputStream );

        //While this is more wordy it allows great seperation.
        List<File> connectionsToExport = new ArrayList<>(  );
        File connectionFileToBeExported = new File ("tempConnectionExportPart1");
        connectionFileToBeExported.deleteOnExit();
        FileOutputStream fileConnectionOutputStream = new FileOutputStream( connectionFileToBeExported );
        OutputStream connectionOutputStream = new BufferedOutputStream( fileConnectionOutputStream );
        connectionsToExport.add( connectionFileToBeExported );

        JsonGenerator connectionJsonGeneration = getJsonGenerator( connectionOutputStream );


        if(exportFilter.getCollections() == null || exportFilter.getCollections().isEmpty()) {
            Map<String, Object> metadata = em.getApplicationCollectionMetadata();

            exportCollection( export, jobExecution, exportFilter, em, starting_time, entitiesExportedCount,
                entitiesToExport, jg, connectionsToExport, connectionJsonGeneration, metadata.keySet() );
        }
        //handles the case where there are specific collections that need to be exported.
        else{
            Set<String> collectionSet = exportFilter.getCollections();

            //loop through only the collection names
            jg = exportCollection( export, jobExecution, exportFilter, em, starting_time, entitiesExportedCount,
                entitiesToExport, jg, connectionsToExport, connectionJsonGeneration, collectionSet );
        }
        jg.flush();
        jg.close();

        HashMap<String,List> filePointers = new HashMap<>(  );
        filePointers.put( "entities",entitiesToExport );
        filePointers.put( "connections",connectionsToExport );

        return filePointers;
    }


    private JsonGenerator exportCollection( final Export export, final JobExecution jobExecution,
                                            final ExportFilter exportFilter, final EntityManager em, long starting_time,
                                            int entitiesExportedCount, final List<File> entitiesToExport,
                                            JsonGenerator jg, final List<File> connectionsToExport,
                                            JsonGenerator connectionJsonGeneration, final Set<String> collectionSet )
        throws Exception {
        final File entityFileToBeExported;
        final FileOutputStream fileEntityOutputStream;
        final OutputStream entityOutputStream;
        final File connectionFileToBeExported;
        final FileOutputStream fileConnectionOutputStream;
        final OutputStream connectionOutputStream;
        for( String collectionName : collectionSet ) {
            //Query entity manager for the entities in a collection
            Query query = null;
            if(exportFilter.getQuery() == null ) {
                query = new Query();
            }
            else {
                try {
                    query = exportFilter.getQuery();
                }
                catch ( Exception e ) {
                    export.setErrorMessage( e.getMessage() );
                }
            }
            query.setLimit( MAX_ENTITY_FETCH );
            query.setResultsLevel( Level.ALL_PROPERTIES );
            query.setCollection( collectionName );

            Results entities = em.searchCollection( em.getApplicationRef(), collectionName, query );

            PagingResultsIterator itr = new PagingResultsIterator( entities );
            //counter that will inform when we should split into another file.
            int currentFilePartIndex = 1;

            for ( Object e : itr ) {
                starting_time = checkTimeDelta( starting_time, jobExecution );
                Entity entity = ( Entity ) e;
                jg.writeObject( entity );
                saveCollectionMembers( jg, em, collectionName, entity );
                saveConnections( entity, em, connectionJsonGeneration,exportFilter );
                jg.writeRaw( '\n' );
                jg.flush();
                entitiesExportedCount++;
                if ( entitiesExportedCount % 1000 == 0 ) {
                    //Time to split files
                    currentFilePartIndex++;
                    entityFileToBeExported = new File( "tempEntityExportPart" + currentFilePartIndex );
                    entityFileToBeExported.deleteOnExit();
                    fileEntityOutputStream = new FileOutputStream( entityFileToBeExported );
                    entityOutputStream = new BufferedOutputStream( fileEntityOutputStream );
                    entitiesToExport.add( entityFileToBeExported );

                    jg = getJsonGenerator( entityOutputStream );


                    //It is quite likely that there are files that do not contain any connections and thus there will not


                    //be anything to write to these empty connection files. Not entirely sure what to do
                    // about that at this point.
                    connectionFileToBeExported = new File( "tempConnectionExportPart" + currentFilePartIndex );
                    connectionFileToBeExported.deleteOnExit();
                    fileConnectionOutputStream = new FileOutputStream( connectionFileToBeExported );
                    connectionOutputStream = new BufferedOutputStream( fileConnectionOutputStream );
                    connectionJsonGeneration = getJsonGenerator( connectionOutputStream );
                    connectionsToExport.add( connectionFileToBeExported );
                }
            }
        }
        return jg;
    }


    private S3Export s3ExportDeterminator(final JobData jobData){
        Object s3PlaceHolder = jobData.getProperty( "s3Export" );
        S3Export s3Export = null;

        try {
            if ( s3PlaceHolder != null ) {
                s3Export = ( S3Export ) s3PlaceHolder;
            }
            else {
                s3Export = new AwsS3ExportImpl();
            }
        }
        catch ( Exception e ) {
            logger.error( "S3Export doesn't exist." );
            throw e;
        }
        return s3Export;
    }

    public void updateExportStatus(Export exportEntity,Export.State exportState,String failureString) throws Exception{
        if(failureString != null)
            exportEntity.setErrorMessage( failureString );

        exportEntity.setState( exportState );

        try {
            em.update( exportEntity );
        }catch(Exception e){
            logger.error( "Encountered error updating export entity! " + e.getMessage() );
            throw e;
        }
    }

    public ExportFilter parseFilterInformation(JobData jobData){
        ExportFilter exportFilter = ( ExportFilter ) jobData.getProperty("filter");
        return exportFilter;
    }
}
