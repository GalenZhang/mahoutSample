package com.achievo.hadoop.storm.logtopology.common;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Conf.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: Conf.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Conf
{
	public static final String REDIS_HOST_KEY = "redisHost";

	public static final String REDIS_PORT_KEY = "redisPort";

	public static final String ELASTIC_CLUSTER_NAME = "ElasticClusterName";

	public static final String DEFAULT_ELASTIC_CLUSTER = "LogStorm";

	public static final String COUNT_CF_NAME = "LogVolumeByMinute";

	public static final String LOGGING_KEYSPACE = "Logging";

	public static final String DEFAULT_JEDIS_PORT = "6379";
}

/*
 * $Log: av-env.bat,v $
 */