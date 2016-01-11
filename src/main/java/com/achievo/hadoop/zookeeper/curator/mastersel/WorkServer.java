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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example leader selector client. Note that {@link LeaderSelectorListenerAdapter} which
 * has the recommended handling for connection state issues
 */
public class WorkServer extends LeaderSelectorListenerAdapter implements Closeable
{
	
	private static final Logger logger = LoggerFactory.getLogger(WorkServer.class);
    private final String name;
    private final LeaderSelector leaderSelector;
    private RunningListener listener; 

    public RunningListener getListener() {
		return listener;
	}

	public void setListener(RunningListener listener) {
		this.listener = listener;
	}

	public WorkServer(CuratorFramework client, String path, String name)
    {
        this.name = name;

        // create a leader selector using the given path for management
        // all participants in a given leader selection must use the same path
        // ExampleClient here is also a LeaderSelectorListener but this isn't required
        leaderSelector = new LeaderSelector(client, path, this);

        // for most cases you will want your instance to requeue when it relinquishes leadership
        leaderSelector.autoRequeue();
    }

    public void start() throws IOException
    {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        
    	leaderSelector.start();
    	processStart(this.name);
    }

    public void close() throws IOException
    {
        leaderSelector.close();
        processStop(this.name);
    }

    public void takeLeadership(CuratorFramework client) throws Exception
    {

        processActiveEnter(this.name);
        
        try
        {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        }
        catch ( InterruptedException e )
        {
            System.err.println(name + " was interrupted.");
            Thread.currentThread().interrupt();
        }
        finally
        {
            //System.out.println(name + " relinquishing leadership.\n");
            processActiveExit(this.name);
        }
    }
    
	private void processStart(Object context) {
		if (listener != null) {
			try {
				listener.processStart(context);
			} catch (Exception e) {
				logger.error("processStart failed", e);
			}
		}
	}

	private void processStop(Object context) {
		if (listener != null) {
			try {
				listener.processStop(context);
			} catch (Exception e) {
				logger.error("processStop failed", e);
			}
		}
	}

	private void processActiveEnter(Object context) {
		if (listener != null) {
			try {
				listener.processActiveEnter(context);
			} catch (Exception e) {
				logger.error("processActiveEnter failed", e);
			}
		}
	}

	private void processActiveExit(Object context) {
		if (listener != null) {
			try {
				listener.processActiveExit(context);
			} catch (Exception e) {
				logger.error("processActiveExit failed", e);
			}
		}
	}


}
