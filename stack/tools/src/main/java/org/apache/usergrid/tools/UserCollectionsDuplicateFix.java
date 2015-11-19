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

package org.apache.usergrid.tools;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;


/**
 * Fixes in 2.0 the issues where we have two unique entities when there should only exist one. Does indepth check
 * on user to verify the right one to delete then deletes it from the keyspace!
 *
 *
 * java -jar usergrid-tools.jar UserCollectionDuplicateFix -host <cassandra_host> -col <Collection to evaluate for merging or deletion>
 */
public class UserCollectionsDuplicateFix extends ToolBase {

    private static final int PAGE_SIZE = 100;

    private static final Logger logger = LoggerFactory.getLogger( UniqueIndexCleanup.class );


    private static final String COLLECTION_ARG = "col";

    @Override
    @SuppressWarnings("static-access")
    public Options createOptions() {
        Options options = new Options();

        Option hostOption =
            OptionBuilder.withArgName( "host" ).hasArg().isRequired( true ).withDescription( "Cassandra host" )
                         .create( "host" );


        options.addOption( hostOption );

        Option collectionOption = OptionBuilder.withArgName( COLLECTION_ARG ).hasArg().isRequired( true )
                                               .withDescription( "collection name" ).create( COLLECTION_ARG );

        options.addOption( collectionOption );

        return options;
    }


    @Override
    public void runTool( final CommandLine line ) throws Exception {

    }
}
