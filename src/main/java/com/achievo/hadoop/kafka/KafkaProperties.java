package com.achievo.hadoop.kafka;
/**
 * <pre>
 * 
 *  Accela Automation
 *  File: KafkaProperties.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: KafkaProperties.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 6, 2015		galen.zhang		Initial.
 *  
 * </pre>
 */
public interface KafkaProperties
{
	final static String zkConnect = "10.50.90.28:2181";
	final static String groupId = "group1";
	final static String topic = "topic1";
	final static String kafkaServerURL = "10.50.90.28";
	final static int kafkaServerPort = 9092;
	final static int kafkaProducerBufferSize = 64 * 1024;
	final static int connectionTimeOut = 20000;
	final static int reconnectInterval = 10000;
	final static String topic2 = "topic2";
	final static String topic3 = "topic3";
	final static String clientId = "SimpleConsumerDemoClient";
	
}

/*
*$Log: av-env.bat,v $
*/