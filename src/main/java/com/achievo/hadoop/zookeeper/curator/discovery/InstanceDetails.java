package com.achievo.hadoop.zookeeper.curator.discovery;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: InstanceDetails.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: InstanceDetails.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class InstanceDetails
{
	private String description;

	public InstanceDetails()
	{
		this("");
	}

	public InstanceDetails(String description)
	{
		this.description = description;
	}

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}

}

/*
 * $Log: av-env.bat,v $
 */