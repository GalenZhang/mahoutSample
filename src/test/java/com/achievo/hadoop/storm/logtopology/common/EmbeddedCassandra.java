package com.achievo.hadoop.storm.logtopology.common;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: EmbeddedCassandra.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: EmbeddedCassandra.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class EmbeddedCassandra
{
	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCassandra.class);

	public static final int DEFAULT_PORT = 9160;

	public static final int DEFAULT_STORAGE_PORT = 7000;

	private final ExecutorService service = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
			.setDaemon(true).setNameFormat("EmbeddedCassandra-%d").build());

	private CassandraDaemon cassandra;

	public EmbeddedCassandra() throws IOException
	{
		this(createTempDir(), "TestCluster", DEFAULT_PORT, DEFAULT_STORAGE_PORT);
	}

	public EmbeddedCassandra(int port) throws IOException
	{
		this(createTempDir(), "TestCluster", port, DEFAULT_STORAGE_PORT);
	}

	public EmbeddedCassandra(File dataDir, String clusterName, int port, int storagePort) throws IOException
	{
		LOG.info("Starting cassandra in dir " + dataDir);
		dataDir.mkdirs();

		URL templateUrl = ClassLoader.getSystemClassLoader().getResource("cassandra-template.yaml");
		Preconditions.checkNotNull(templateUrl, "Cassandra config template is null");

		List<Object> urlList = new ArrayList<Object>();
		urlList.add(templateUrl);
		String baseFile = Resources.toString(urlList);

		String newFile = baseFile.replace("$DIR$", dataDir.getPath());
		newFile = newFile.replace("$PORT$", Integer.toString(port));
		newFile = newFile.replace("$STORAGE_PORT$", Integer.toString(storagePort));
		newFile = newFile.replace("$CLUSTER$", clusterName);

		File configFile = new File(dataDir, "cassandra.yaml");
		Files.write(newFile, configFile, Charset.defaultCharset());

		LOG.info("Cassandra config file: " + configFile.getPath());
		System.setProperty("cassandra.config", "file://" + configFile.getParent());

		try
		{
			cassandra = new CassandraDaemon();
			cassandra.init(null);
		}
		catch (IOException e)
		{
			LOG.error("Error initializing embedded cassandra", e);
			throw e;
		}

		LOG.info("Started cassandra daemon");
	}

	private static File createTempDir()
	{
		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		return tempDir;
	}

	public void start() throws IOException
	{
		service.submit(new Callable<Object>()
		{
			@Override
			public Object call() throws Exception
			{
				try
				{
					cassandra.start();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				return null;
			}

		});
	}

	public void stop()
	{
		service.shutdownNow();
		cassandra.deactivate();
	}
}

/*
 * $Log: av-env.bat,v $
 */