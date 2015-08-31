package com.achievo.hadoop.zookeeper.curator.locking;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ExampleClientThatLocks.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ExampleClientThatLocks.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ExampleClientThatLocks
{
	private final InterProcessMutex lock;

	private final FakeLimitedResource resource;

	private final String clientName;

	public ExampleClientThatLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource,
			String clientName)
	{
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessMutex(client, lockPath);
	}

	public void doWork(long time, TimeUnit unit) throws Exception
	{
		if (!lock.acquire(time, unit))
		{
			throw new IllegalStateException(clientName + " could not acquire the lock");
		}

		try
		{
			System.out.println(clientName + " has the lock");
			resource.use();
		}
		finally
		{
			System.out.println(clientName + " releasing the lock");
			lock.release(); // always release the lock in a finally block
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */