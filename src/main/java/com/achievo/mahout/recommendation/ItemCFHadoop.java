package com.achievo.mahout.recommendation;
/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ItemCFHadoop.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ItemCFHadoop.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 2, 2015		galen.zhang		Initial.
 *  
 * </pre>
 */
public class ItemCFHadoop
{
	private static final String HDFS = "hdfs://localhost:9000";
	
	public static void main(String[] args)
	{
		String localFile = "datafile/item.csv";
		String inPath = HDFS + "/user/hadoop/userCF";
		String inFile = inPath + "/item.csv";
		String outPath = HDFS + "/user/hadoop/userCF/result/";
		String outFile = outPath + "/part-r-00000";
		String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();
	}
}

/*
*$Log: av-env.bat,v $
*/