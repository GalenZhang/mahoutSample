package com.achievo.hadoop.zookeeper.curator.framework;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: CrudExamples.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: CrudExamples.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class CrudExamples
{
	public static void create(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		client.create().forPath(path, payload);
	}

	public static void createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
	}

	public static void createEphemeralSequential(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
	}

	public static void setData(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		client.setData().forPath(path, payload);
	}

	public static void setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		CuratorListener listener = new CuratorListener()
		{
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
			{
				// examine event for details
			}
		};
		client.getCuratorListenable().addListener(listener);
		client.setData().inBackground().forPath(path, payload);
	}

	public static void delete(CuratorFramework client, String path) throws Exception
	{
		client.delete().forPath(path);
	}

	public static void guaranteedDelete(CuratorFramework client, String path) throws Exception
	{
		client.delete().guaranteed().forPath(path);
	}

	public static List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception
	{
		return client.getChildren().watched().forPath(path);
	}
	
	public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher) throws Exception
	{
		return client.getChildren().usingWatcher(watcher).forPath(path);
	}
}

/*
 * $Log: av-env.bat,v $
 */