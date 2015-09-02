package com.achievo.hadoop.zookeeper.curator.locking;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ExampleClientReadWriteLocks.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ExampleClientReadWriteLocks.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ExampleClientReadWriteLocks
{
	private final InterProcessReadWriteLock lock;

	private final InterProcessMutex readLock;

	private final InterProcessMutex writeLock;

	private final FakeLimitedResource resource;

	private final String clientName;

	public ExampleClientReadWriteLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource,
			String clientName)
	{
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessReadWriteLock(client, lockPath);
		readLock = lock.readLock();
		writeLock = lock.writeLock();
	}

	public void doWork(long time, TimeUnit unit) throws Exception
	{
		if (!writeLock.acquire(time, unit))
		{
			throw new IllegalStateException(clientName + " could not acquire the writeLock");
		}
		System.out.println(clientName + " has the writeLock");
		
		if (!readLock.acquire(time, unit))
		{
			throw new IllegalStateException(clientName + " could not acquire the readLock");
		}
		System.out.println(clientName + " has the readLock too");
		
		try
		{
			resource.use();
		}
		finally
		{
			System.out.println(clientName + " releasing the lock");
			readLock.release();
			writeLock.release();
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */