package com.achievo.hadoop.kafka;

import kafka.producer.Partitioner;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SimplePartitioner.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SimplePartitioner.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SimplePartitioner implements Partitioner
{

	@Override
	public int partition(Object key, int a_numPartitions)
	{
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0)
		{
			partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}

}

/*
 * $Log: av-env.bat,v $
 */