package com.achievo.hadoop.zookeeper.curator.cache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import com.achievo.hadoop.zookeeper.curator.discovery.ExampleServer;
import com.google.common.collect.Lists;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: PathCacheExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: PathCacheExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class PathCacheExample
{
	private static final String PATH = "/example/cache";

	public static void main(String[] args) throws Exception
	{
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		PathChildrenCache cache = null;

		try
		{
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.start();

			cache = new PathChildrenCache(client, PATH, true);
			cache.start();
			
			processCommands(client, cache);
		}
		finally
		{
			CloseableUtils.closeQuietly(cache);
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}

	private static void addListener(PathChildrenCache cache)
	{
		PathChildrenCacheListener listener = new PathChildrenCacheListener()
		{
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
			{
				switch (event.getType())
				{
					case CHILD_ADDED:
					{
						System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
						break;
					}
					case CHILD_UPDATED:
					{
						System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
						break;
					}
					case CHILD_REMOVED:
					{
						System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
						break;
					}
				}
			}
		};
		cache.getListenable().addListener(listener);
	}

	private static void processCommands(CuratorFramework client, PathChildrenCache cache) throws Exception
	{
		printHelp();
		
		List<ExampleServer> servers = Lists.newArrayList();
		try
		{
			addListener(cache);
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			boolean done = false;
			
			while (!done)
			{
				System.out.print(">  ");
				
				String line = in.readLine();
				if (line == null)
				{
					break;
				}
				
				String command = line.trim();
				String[] parts = command.split("\\s");
				if (parts.length == 0)
				{
					continue;
				}
				
				String operation = parts[0];
				String args[] = Arrays.copyOfRange(parts, 1, parts.length);
				
				if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("quit"))
				{
					done = true;
				}
				else if (operation.equals("set"))
				{
					setValue(client, command, args);
				}
				else if (operation.equals("remove"))
				{
					remove(client, command, args);
				}
				else if (operation.equals("list"))
				{
					list(cache);
				}
				
				Thread.sleep(1000); // just to allow the console output to catch up
			}
		}
		finally
		{
			for (ExampleServer server : servers)
			{
				CloseableUtils.closeQuietly(server);
			}
		}
		
	}
	
	private static void list(PathChildrenCache cache)
	{
		if (cache.getCurrentData().size() ==0)
		{
			System.out.println("*  empty  *");
		}
		else
		{
			for (ChildData data : cache.getCurrentData())
			{
				System.out.println(data.getPath() + " = " + new String(data.getData()));
			}
		}
	}
	
	private static void remove(CuratorFramework client, String command, String[] args) throws Exception
	{
		if (args.length != 1)
		{
			System.err.println("syntax error (expected remove <path>): " + command);
			return;
		}
		
		String name = args[0];
		if (name.contains("/"))
		{
			System.err.println("Invalid node name " + name);
			return;
		}
		String path = ZKPaths.makePath(PATH, name);
		
		try
		{
			client.delete().forPath(path);
		}
		catch (KeeperException.NoNodeException e)
		{
			e.printStackTrace();
		}
	}
	
	private static void setValue(CuratorFramework client, String command, String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("syntax error (expected set <path> <value>): " + command);
			return;
		}
		
		String name = args[0];
		if (name.contains("/"))
		{
			System.err.println("Invalid node name " + name);
			return;
		}
		String path = ZKPaths.makePath(PATH, name);
		byte[] bytes = args[1].getBytes();
		
		try
		{
			client.setData().forPath(path, bytes);
		}
		catch (KeeperException.NoNodeException e)
		{
			client.create().creatingParentsIfNeeded().forPath(path, bytes);
		}
	}

	private static void printHelp()
	{
		System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
		System.out.println("set <name> <value>: Adds or updates a node with the given name");
		System.out.println("remove <name>: Deletes the node with the given name");
		System.out.println("list: List the nodes/values in the cache");
		System.out.println("quit: Quit the example");
		System.out.println();
	}
}

/*
 * $Log: av-env.bat,v $
 */