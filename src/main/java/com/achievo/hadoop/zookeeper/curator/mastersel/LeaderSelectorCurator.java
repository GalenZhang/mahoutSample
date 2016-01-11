/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.achievo.hadoop.zookeeper.curator.mastersel;

import com.google.common.collect.Lists;

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class LeaderSelectorCurator
{
    private static final int        CLIENT_QTY = 10;

    private static final String     PATH = "/leader";

    public static void main(String[] args) throws Exception
    {
        // all of the useful sample code is in ExampleClient.java

        List<CuratorFramework>  clients = Lists.newArrayList();
        List<WorkServer>     workServers = Lists.newArrayList();

        try
        {
            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                CuratorFramework    client = CuratorFrameworkFactory.newClient("192.168.1.105:2181", new ExponentialBackoffRetry(1000, 3));
                clients.add(client);

                WorkServer       workServer = new WorkServer(client, PATH, "Client #" + i);
                workServer.setListener(new RunningListener() {
					
					public void processStop(Object context) {
						System.out.println(context.toString()+"processStop...");				
					}
					
					public void processStart(Object context) {
						System.out.println(context.toString()+"processStart...");						
					}
					
					public void processActiveExit(Object context) {
						System.out.println(context.toString()+"processActiveExit...");						
					}
					
					public void processActiveEnter(Object context) {
						System.out.println(context.toString()+"processActiveEnter...");						
					}
				});
                
                workServers.add(workServer);

                client.start();
                workServer.start();
            }

            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        finally
        {
            System.out.println("Shutting down...");

            for ( WorkServer workServer : workServers )
            {
                CloseableUtils.closeQuietly(workServer);
            }
            for ( CuratorFramework client : clients )
            {
                CloseableUtils.closeQuietly(client);
            }
        }
    }
}
