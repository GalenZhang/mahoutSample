package com.achievo.hadoop.zookeeper.curator.cache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: NodeCacheExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: NodeCacheExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 2, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class NodeCacheExample
{
	private static final String PATH = "/examples/nodeCache";

	public static void main(String[] args)
	{
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		NodeCache cache = null;

		try
		{
			client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
			client.start();
			cache = new NodeCache(client, PATH);
			cache.start();
			processCommands(client, cache);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			CloseableUtils.closeQuietly(cache);
			CloseableUtils.closeQuietly(client);
		}
	}

	private static void addListener(final NodeCache cache)
	{
		NodeCacheListener listener = new NodeCacheListener()
		{
			@Override
			public void nodeChanged() throws Exception
			{
				if (cache.getCurrentData() != null)
				{
					System.out.println("Node changed: " + cache.getCurrentData().getPath() + ", value: "
							+ new String(cache.getCurrentData().getData()));
				}
			}
		};
		cache.getListenable().addListener(listener);
	}

	private static void processCommands(CuratorFramework client, NodeCache cache)
	{
		printHelp();
		try
		{
			addListener(cache);
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			boolean done = false;

			while (!done)
			{
				System.out.print("> ");
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
				String[] args = Arrays.copyOfRange(parts, 1, parts.length);
				if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?"))
				{
					printHelp();
				}
				else if (operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit"))
				{
					done = true;
				}
				else if (operation.equals("set"))
				{
					setValue(client, command, args);
				}
				else if (operation.equals("remove"))
				{
					remove(client);
				}
				else if (operation.equals("show"))
				{
					show(cache);
				}
				Thread.sleep(1000);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			CloseableUtils.closeQuietly(cache);
			CloseableUtils.closeQuietly(client);
		}

	}

	private static void show(NodeCache cache)
	{
		if (cache.getCurrentData() != null)
		{
			System.out.println(cache.getCurrentData().getPath() + " = " + new String(cache.getCurrentData().getData()));
		}
		else
		{
			System.out.println("cache don't set a value");
		}
	}

	private static void remove(CuratorFramework client) throws Exception
	{
		try
		{
			client.delete().forPath(PATH);
		}
		catch (KeeperException.NoNodeException e)
		{
			e.printStackTrace();
		}
	}

	private static void setValue(CuratorFramework client, String command, String[] args) throws Exception
	{
		if (args.length != 1)
		{
			System.err.println("syntax error (expected set <value>): " + command);
			return;
		}

		byte[] bytes = args[0].getBytes();
		try
		{
			client.setData().forPath(PATH, bytes);
		}
		catch (KeeperException.NoNodeException e)
		{
			client.create().creatingParentsIfNeeded().forPath(PATH, bytes);
		}
	}

	private static void printHelp()
	{
		System.out
				.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
		System.out.println("set <value>: Adds or updates a node with the given name");
		System.out.println("remove: Deletes the node with the given name");
		System.out.println("show: Display the node's value in the cache");
		System.out.println("quit: Quit the example");
		System.out.println();
	}
}

/*
 * $Log: av-env.bat,v $
 */