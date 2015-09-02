package com.achievo.hadoop.zookeeper.curator.leader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ExampleClient.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ExampleClient.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable
{
	private final String name;
	private final LeaderSelector leaderSelector;
	private final AtomicInteger leaderCount = new AtomicInteger();

	public ExampleClient(CuratorFramework client, String path, String name)
	{
		this.name = name;
		leaderSelector = new LeaderSelector(client, path, this);
		leaderSelector.autoRequeue();
	}

	public void start() throws IOException
	{
		leaderSelector.start();
	}

	public void takeLeadership(CuratorFramework client) throws Exception
	{
		final int waitSeconds = (int) (5 * Math.random()) + 1;

		System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
		System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");

		try
		{
			Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
		}
		catch (InterruptedException e)
		{
			System.err.println(name + " was interrupted.");
			Thread.currentThread().interrupt();
		}
		finally
		{
			System.out.println(name + " relinquishing leadership.\n");
		}
	}

	public void close() throws IOException
	{
		leaderSelector.close();
	}

}

/*
 * $Log: av-env.bat,v $
 */