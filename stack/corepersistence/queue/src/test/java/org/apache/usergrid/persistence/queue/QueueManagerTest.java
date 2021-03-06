/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.persistence.queue;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.usergrid.persistence.core.aws.NoAWSCredsRule;
import org.apache.usergrid.persistence.core.test.ITRunner;
import org.apache.usergrid.persistence.core.test.UseModules;
import org.apache.usergrid.persistence.queue.guice.TestQueueModule;
import org.apache.usergrid.persistence.queue.impl.QueueScopeImpl;

import com.google.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


@RunWith( ITRunner.class )
@UseModules( { TestQueueModule.class } )
public class QueueManagerTest {

    @Inject
    protected QueueFig queueFig;
    @Inject
    protected QueueManagerFactory qmf;

    /**
     * Mark tests as ignored if no AWS creds are present
     */
    @Rule
    public NoAWSCredsRule awsCredsRule = new NoAWSCredsRule();


    protected QueueScope scope;
    private QueueManager qm;

    public static long queueSeed = System.currentTimeMillis();


    @Before
    public void mockApp() {

        this.scope = new QueueScopeImpl( "testQueue"+queueSeed++, QueueScope.RegionImplementation.LOCAL);
        qm = qmf.getQueueManager(scope);
    }

    @org.junit.After
    public void cleanup(){
        qm.deleteQueue();
    }


    @Test
    public void send() throws Exception{
        String value = "bodytest";
        qm.sendMessage(value);
        List<QueueMessage> messageList = qm.getMessages(1,5000,5000,String.class);
        assertTrue(messageList.size() >= 1);
        for(QueueMessage message : messageList){
            assertTrue(message.getBody().equals(value));
            qm.commitMessage(message);
        }

        messageList = qm.getMessages(1,5000,5000,String.class);
        assertTrue(messageList.size() <= 0);

    }

    @Test
    public void sendMore() throws Exception{
        HashMap<String,String> values = new HashMap<>();
        values.put("test","Test");

        List<Map<String,String>> bodies = new ArrayList<>();
        bodies.add(values);
        qm.sendMessages(bodies);
        List<QueueMessage> messageList = qm.getMessages(1,5000,5000,values.getClass());
        assertTrue(messageList.size() >= 1);
        for(QueueMessage message : messageList){
            assertTrue(message.getBody().equals(values));
        }
        qm.commitMessages(messageList);

        messageList = qm.getMessages(1,5000,5000,values.getClass());
        assertTrue(messageList.size() <= 0);

    }

    @Test
    public void queueSize() throws Exception{
        HashMap<String,String> values = new HashMap<>();
        values.put("test", "Test");

        List<Map<String,String>> bodies = new ArrayList<>();
        bodies.add(values);
        long initialDepth = qm.getQueueDepth();
        qm.sendMessages(bodies);
        long depth = 0;
        for(int i=0; i<10;i++){
             depth = qm.getQueueDepth();
            if(depth>0){
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(depth>0);

        List<QueueMessage> messageList = qm.getMessages(10,5000,5000,values.getClass());
        assertTrue(messageList.size() <= 500);
        for(QueueMessage message : messageList){
            assertTrue(message.getBody().equals(values));
        }
        if(messageList.size()>0) {
            qm.commitMessages(messageList);
        }
        for(int i=0; i<10;i++){
            depth = qm.getQueueDepth();
            if(depth==initialDepth){
                break;
            }
            Thread.sleep(1000);
        }
        assertEquals(initialDepth, depth);
    }



}
