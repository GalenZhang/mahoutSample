package com.achievo.hadoop.storm.logtopology.model;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: NotificationDetails.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: NotificationDetails.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class NotificationDetails
{
	private String to;

	private Severity severity;

	private String message;

	public NotificationDetails(String to, Severity severity, String message)
	{
		this.to = to;
		this.severity = severity;
		this.message = message;
	}

	public String getTo()
	{
		return to;
	}

	public Severity getSeverity()
	{
		return severity;
	}

	public String getMessage()
	{
		return message;
	}

}

/*
 * $Log: av-env.bat,v $
 */