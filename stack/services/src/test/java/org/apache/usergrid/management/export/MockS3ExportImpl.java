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
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Streams/reads the information written from the export service to a file named "test.json"
 */
public class MockS3ExportImpl implements S3Export {
    private static final Logger logger = LoggerFactory.getLogger( MockS3ExportImpl.class );

    private final String filename;


    public MockS3ExportImpl (String filename) {
        this.filename = filename;
    }


    @Override
    public void copyToS3( Map ephemeral, final Map<String,Object> exportInfo, String ignoredFileName ) {

        List<File> entityFilesToBeExported = ( List<File> ) ephemeral.get( "entities" );
        List<File> connectionFilesToBeExported = (List<File>) ephemeral.get("connections");

        int fileCounter = 1;
        for(File fileToBeWritten: entityFilesToBeExported ) {
            File verifiedData = new File( "entities"+fileCounter+filename );
            try {
                FileUtils.copyFile( fileToBeWritten, verifiedData );
                fileCounter++;
                logger.info( "Copied file {} to {}", fileToBeWritten.getAbsolutePath(), verifiedData );
            }
            catch ( IOException e ) {
                e.printStackTrace();
            }
        }

        fileCounter = 1;
        for(File fileToBeWritten: connectionFilesToBeExported ) {
            File verifiedData = new File("connections"+fileCounter+ filename );
            try {
                FileUtils.copyFile( fileToBeWritten, verifiedData );
                fileCounter++;
                logger.info( "Copied file {} to {}", fileToBeWritten.getAbsolutePath(), verifiedData );
            }
            catch ( IOException e ) {
                e.printStackTrace();
            }
        }
    }
}
