package com.achievo.hadoop.zookeeper.curator.framework;

import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: TransactionExamples.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TransactionExamples.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TransactionExamples
{
	public static Collection<CuratorTransactionResult> transaction(CuratorFramework client) throws Exception
	{
		// this example shows how to use ZooKeeper's new transactions
		Collection<CuratorTransactionResult> results = client.inTransaction().create()
				.forPath("/a/path", "some data".getBytes()).and().setData()
				.forPath("/another/path", "other data".getBytes()).and().delete().forPath("/yet/another/path").and()
				.commit();

		for (CuratorTransactionResult result : results)
		{
			System.out.println(result.getForPath() + " - " + result.getType());
		}

		return results;
	}

	public static CuratorTransaction startTransaction(CuratorFramework client)
	{
		return client.inTransaction();
	}

	public static CuratorTransactionFinal addCreateToTransaction(CuratorTransaction transaction) throws Exception
	{
		return transaction.create().forPath("/a/path", "some data".getBytes()).and();
	}

	public static CuratorTransactionFinal addDeleteToTransaction(CuratorTransaction transaction) throws Exception
	{
		return transaction.delete().forPath("/another/path").and();
	}

	public static void commitTransaction(CuratorTransactionFinal transaction) throws Exception
	{
		transaction.commit();
	}
}

/*
 * $Log: av-env.bat,v $
 */