package com.achievo.hadoop.zookeeper.message;

import java.io.IOException;

import com.achievo.hadoop.hdfs.HdfsDAO;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Profit.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: Profit.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Profit
{
	public static void main(String[] args) throws NumberFormatException, IOException
	{
		profit();
	}

	public static void profit() throws NumberFormatException, IOException
	{
		int sell = getSell();
		int purchase = getPurchase();
		int other = getOther();
		int profit = sell - purchase - other;
		System.out.printf("profit = sell - purchase - other = %d - %d -%d = %d\n", sell, purchase, other, profit);
	}

	public static int getOther() throws IOException
	{
		return Other.calcOther(Other.file);
	}

	public static int getPurchase() throws NumberFormatException, IOException
	{
		HdfsDAO hdfs = new HdfsDAO(Purchase.HDFS, Purchase.config());
		return Integer.parseInt(hdfs.cat(Purchase.path().get("output") + "/part-r-00000").trim());
	}

	public static int getSell() throws NumberFormatException, IOException
	{
		HdfsDAO hdfs = new HdfsDAO(Sell.HDFS, Sell.config());
		return Integer.parseInt(hdfs.cat(Sell.path().get("output") + "/part-r-00000").trim());
	}
}

/*
 * $Log: av-env.bat,v $
 */