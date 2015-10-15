package com.achievo.hadoop.storm.logtopology.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.achievo.hadoop.storm.logtopology.IntegerationTestTopology;
import com.achievo.hadoop.storm.logtopology.model.LogEntry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: UnitTestUtils.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: UnitTestUtils.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class UnitTestUtils
{
	public static String readFile(String file) throws IOException
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				IntegerationTestTopology.class.getResourceAsStream(file)));

		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null)
		{
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		return stringBuilder.toString();
	}

	public static LogEntry getEntry() throws IOException
	{
		String testData = UnitTestUtils.readFile("/testData1.json");
		JSONObject obj = (JSONObject) JSONValue.parse(testData);

		return new LogEntry(obj);
	}
}

/*
 * $Log: av-env.bat,v $
 */