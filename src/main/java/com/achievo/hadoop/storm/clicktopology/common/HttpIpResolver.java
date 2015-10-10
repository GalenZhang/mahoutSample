package com.achievo.hadoop.storm.clicktopology.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HttpIpResolver.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: HttpIpResolver.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HttpIpResolver implements IPResolver, Serializable
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 358562100138768918L;

	private static String url = "http://api.hostip.info/get_json.php";

	@Override
	public JSONObject resolveIP(String ip)
	{
		URL geoUrl = null;
		BufferedReader in = null;

		try
		{
			geoUrl = new URL(url + "?ip=" + ip);
			URLConnection connection = geoUrl.openConnection();
			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			JSONObject json = (JSONObject) JSONValue.parse(in);
			return json;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (in != null)
			{
				try
				{
					in.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static void main(String[] args)
	{
		HttpIpResolver resolver = new HttpIpResolver();
		System.out.println(resolver.resolveIP("220.232.135.194"));
	}
}

/*
 * $Log: av-env.bat,v $
 */