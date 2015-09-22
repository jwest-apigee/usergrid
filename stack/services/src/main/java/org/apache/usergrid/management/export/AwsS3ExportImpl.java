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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.services.assets.data.AssetUtils;
import org.apache.usergrid.utils.StringUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;


/**
 * An implementation that doesn't use Jclouds and instead uses the Aws Java Sdk. Copied the given ephmeral file over
 * to s3.
 */
public class AwsS3ExportImpl implements S3Export {
    private AmazonS3 s3Client;
    private static final long FIVE_MB = ( FileUtils.ONE_MB * 5 );



    private static final Logger logger = LoggerFactory.getLogger( AwsS3ExportImpl.class );

    //exportInfo Should probably just be an object class that represents the data
    //refer to the tomb of programming stuff to verify.
    @Override
    public void copyToS3( final Map ephemeral, final Map<String, Object> exportInfo, final String filename ) throws Exception {

        //Get bucketname, access key, and secret key
        logger.debug( "Getting bucketname, access key, and secret key" );

        //TODO:GREY refactor to use objects instead of maps.
                /*won't need any of the properties as I have the export info*/
        Map<String,Object> properties = ( Map<String, Object> ) exportInfo.get( "properties" );
        Map<String, Object> storage_info = (Map<String,Object>)properties.get( "storage_info" );

        String bucketName = ( String ) storage_info.get( "bucket_location" );
        String accessId = ( String ) storage_info.get( "s3_access_id");
        String secretKey = ( String ) storage_info.get( "s3_key" );

        //Initialize amazon client using given credentials
        logger.debug( "Initialize amazon client using given credentials" );
        AWSCredentials credentials = new BasicAWSCredentials(accessId, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol( Protocol.HTTP);

        s3Client = new AmazonS3Client(credentials, clientConfig);
        //purposefully commenting out the region stuff as that isn't supported
        //in the rest api but could be some way
//        if(regionName != null)
//            s3Client.setRegion( Region.getRegion( Regions.fromName( regionName ) ) );


        //Give Aws Sdk credentials so we can create the bucketname
        //if it doesn't already exist
        logger.debug("Check if bucketName already exists in s3");
        //pretty much copy and paste AwsSdkS3BinaryStore
        //seems like this might be the same step as just writing something so it shouldn't be an issue


        try {
            s3Client.createBucket( bucketName );
            logger.debug( "Bucket wasn't found so created a new bucket with the name:"+bucketName );
        }catch ( Exception e ){
            logger.error( "creating the bucket failed due to \""+e.getMessage()+"\"");
        }


        //Multipart upload of the file if it is >5mb otherwise
        //we can just upload it if it is a small export
        logger.debug( "Upload the file to amazon s3" );
        //for ( File fileToBeUploaded : ephemeral )
        List<File> filePointersToEntityFiles= ( List<File> ) ephemeral.get( "entities" );
        int index = 1;
        for(File entitiesToExport: filePointersToEntityFiles)
            write( entitiesToExport,"entities"+index+filename,bucketName );

        index = 1;
        List<File> filePointersToConnectionFiles= ( List<File> ) ephemeral.get( "connections" );
        for(File connectionsToExport: filePointersToConnectionFiles)
            write( connectionsToExport, "connections"+index+ filename, bucketName );


    }

    //Needs to take in the ephemeral file and the filename
    public void write(final File ephemeral, final String filename,final String bucketName) throws Exception{


        InputStream fileInputStream = new FileInputStream( ephemeral );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long written = IOUtils.copyLarge( fileInputStream, baos, 0, FIVE_MB );

        byte[] data = baos.toByteArray();

        InputStream awsInputStream = new ByteArrayInputStream(data);

        Boolean overSizeLimit = false;

        if ( written < FIVE_MB ) { // total smaller than 5mb
            logger.debug( "STARTING THE PUT TO AWS");

            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(written);
            PutObjectResult result = null;
            result = s3Client.putObject( bucketName, filename, ephemeral );
            logger.debug( "the results are: "+result.toString() );


        }
        else { // bigger than 5mb... dump 5 mb tmp files and upload from them
            written = 0; //reset written to 0, we still haven't wrote anything in fact
            int partNumber = 1;
            int firstByte = 0;
            Boolean isFirstChunck = true;
            List<PartETag> partETags = new ArrayList<PartETag>();


            //get the s3 client in order to initialize the multipart request
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( bucketName, filename);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload( initRequest );


            InputStream firstChunck = new ByteArrayInputStream(data);
            PushbackInputStream chunckableInputStream = new PushbackInputStream(fileInputStream, 1);

            // determine max size file allowed, default to 50mb
            long maxSizeBytes = 50 * FileUtils.ONE_MB;
//            String maxSizeMbString = properties.getProperty( "usergrid.binary.max-size-mb", "50" );
//            if ( StringUtils.isNumeric( maxSizeMbString )) {
//                maxSizeBytes = Long.parseLong( maxSizeMbString ) * FileUtils.ONE_MB;
//            }

//            // always allow files up to 5mb
//            if (maxSizeBytes < 5 * FileUtils.ONE_MB ) {
//                maxSizeBytes = 5 * FileUtils.ONE_MB;
//            }

            while (-1 != (firstByte = chunckableInputStream.read())) {
                long partSize = 0;
                chunckableInputStream.unread( firstByte );
                File tempFile = ephemeral;
                tempFile.deleteOnExit();
                OutputStream os = null;
                try {
                    os = new BufferedOutputStream( new FileOutputStream( tempFile.getAbsolutePath() ) );

                    if(isFirstChunck == true) {
                        partSize = IOUtils.copyLarge( firstChunck, os, 0, ( FIVE_MB ) );
                        isFirstChunck = false;
                    }
                    else {
                        partSize = IOUtils.copyLarge( chunckableInputStream, os, 0, ( FIVE_MB ) );
                    }
                    written += partSize;

                    if(written> maxSizeBytes){
                        overSizeLimit = true;
                        logger.error( "OVERSIZED FILE. STARTING ABORT" );
                        break;
                        //set flag here and break out of loop to run abort
                    }
                }
                finally {
                    IOUtils.closeQuietly( os );
                }

                FileInputStream chunk = new FileInputStream(tempFile);

                Boolean isLastPart = -1 == (firstByte = chunckableInputStream.read());
                if(!isLastPart)
                    chunckableInputStream.unread(firstByte);

                UploadPartRequest uploadRequest = new UploadPartRequest().withUploadId(initResponse.getUploadId())
                                                                         .withBucketName(bucketName)
                                                                         .withKey(filename)
                                                                         .withInputStream(chunk)
                                                                         .withPartNumber(partNumber)
                                                                         .withPartSize(partSize)
                                                                         .withLastPart(isLastPart);
                partETags.add( s3Client.uploadPart( uploadRequest ).getPartETag() );
                partNumber++;
            }
        }
        logger.debug( "Done uploading" );
    }
}
