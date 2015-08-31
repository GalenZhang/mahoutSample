package com.achievo.hadoop.zookeeper.curator.locking;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: FakeLimitedResource.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: FakeLimitedResource.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class FakeLimitedResource
{
	private final AtomicBoolean inUse = new AtomicBoolean(false);

	public void use() throws InterruptedException
	{
		// in a real application this would be accessing/manipulating a shared resource
		if (!inUse.compareAndSet(false, true))
		{
			throw new IllegalStateException("Needs to be used by one client at a time");
		}
		
		try
		{
			Thread.sleep((long)(3 * Math.random()));
		}
		finally
		{
			inUse.set(false);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */