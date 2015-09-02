package com.achievo.hadoop.zookeeper.curator.leader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: LeaderSelectorExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: LeaderSelectorExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class LeaderSelectorExample
{
	private static final int CLIENT_QTY = 10;

	private static final String PATH = "/examples/leader";

	public static void main(String[] args) throws Exception
	{
		System.out
				.println("Create "
						+ CLIENT_QTY
						+ " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.");
		System.out
				.println("Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

		List<CuratorFramework> clients = Lists.newArrayList();
		List<ExampleClient> examples = Lists.newArrayList();
		TestingServer server = new TestingServer();

		try
		{
			for (int i = 0; i < CLIENT_QTY; i++)
			{
				CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
					new ExponentialBackoffRetry(1000, 3));
				clients.add(client);

				ExampleClient example = new ExampleClient(client, PATH, "Client #" + i);
				examples.add(example);

				client.start();
				example.start();
			}

			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in));
		}
		finally
		{
			System.out.println("Shutting down...");

			for (ExampleClient exampleClient : examples)
			{
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clients)
			{
				CloseableUtils.closeQuietly(client);
			}

			CloseableUtils.closeQuietly(server);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */