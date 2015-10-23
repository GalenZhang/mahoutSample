package com.achievo.hadoop.storm.trident.tfidf.state;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: StateUtils.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: StateUtils.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class StateUtils
{
	public static String formatHour(Date date)
	{
		return new SimpleDateFormat("yyyyMMddHH").format(date);
	}
}

/*
 * $Log: av-env.bat,v $
 */