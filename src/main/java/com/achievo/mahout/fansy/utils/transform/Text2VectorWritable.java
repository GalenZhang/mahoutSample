package com.achievo.mahout.fansy.utils.transform;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Text2VectorWritable.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: Text2VectorWritable.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Text2VectorWritable extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new Text2VectorWritable(), args);
	}

	public int run(String[] arg0) throws Exception
	{
		// TODO Auto-generated method stub
		return 0;
	}

}

/*
 * $Log: av-env.bat,v $
 */