package com.achievo.hadoop.zookeeper.curator.discovery;

import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ExampleServer.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ExampleServer.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ExampleServer implements Closeable
{
	private final ServiceDiscovery<InstanceDetails> serviceDiscovery;

	private final ServiceInstance<InstanceDetails> thisInstance;

	public ExampleServer(CuratorFramework client, String path, String serviceName, String description) throws Exception
	{
		UriSpec uriSpec = new UriSpec("{scheme}://foo.com:{port}");

		thisInstance = ServiceInstance.<InstanceDetails> builder().name(serviceName)
				.payload(new InstanceDetails(description)).port((int) (65535 * Math.random())).uriSpec(uriSpec).build();

		JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(
				InstanceDetails.class);

		serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class).client(client).basePath(path)
				.serializer(serializer).thisInstance(thisInstance).build();
	}

	public ServiceInstance<InstanceDetails> getThisInstance()
	{
		return thisInstance;
	}

	public void start() throws Exception
	{
		serviceDiscovery.start();
	}

	public void close() throws IOException
	{
		CloseableUtils.closeQuietly(serviceDiscovery);
	}

}

/*
 * $Log: av-env.bat,v $
 */