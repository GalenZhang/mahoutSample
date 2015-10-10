package com.achievo.hadoop.storm.clicktopology.common;

import org.json.simple.JSONObject;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: IPResolver.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: IPResolver.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public interface IPResolver
{
	public JSONObject resolveIP(String ip);
}

/*
 * $Log: av-env.bat,v $
 */