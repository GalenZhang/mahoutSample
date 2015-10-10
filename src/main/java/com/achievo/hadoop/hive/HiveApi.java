package com.achievo.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HiveApi.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://fromwiz.com/share/s/09liu01n_N7y2FJV1W2R6SqK2xw33J3HgkDt2it7Vq0fttl2
 * 
 *  Notes:
 * 	$Id: HiveApi.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 22, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HiveApi
{
	private static String driverName = "org.apache.hive.jdbc.HiveDirver";

	public static void main(String[] args) throws SQLException
	{
		try
		{
			Class.forName(driverName);
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
		}

		Connection conn = DriverManager.getConnection("jdbc:hive2://10.50.90.28:10000/default", "", "");
		Statement stmt = conn.createStatement();
		String tableName = "t_test";
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (key int, value) ");
		System.out.println("Create table success!");

		// show tables
		String sql = "show table '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next())
		{
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next())
		{
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		
		sql = "select * from " + tableName;
		res = stmt.executeQuery(sql);
		if (res.next())
		{
			System.out.println(res.getInt(1) + "\t" + res.getString(2));
		}
		
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next())
		{
			System.out.println(res.getString(1));
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */