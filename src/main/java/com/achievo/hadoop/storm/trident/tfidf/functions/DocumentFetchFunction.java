package com.achievo.hadoop.storm.trident.tfidf.functions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: DocumentFetchFunction.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: DocumentFetchFunction.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class DocumentFetchFunction extends BaseFunction
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -7972333789403411282L;

	private Logger log = LoggerFactory.getLogger(DocumentFetchFunction.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector)
	{
		String url = tuple.getStringByField("url");
		String document = readDocument(url);

		collector.emit(new Values(document, url.trim(), "file"));
	}

	private String readDocument(String url)
	{
		BufferedReader documentReader = null;
		try
		{
			documentReader = new BufferedReader(new FileReader(url));
			String line = null;
			StringBuilder documentBuilder = new StringBuilder();

			while ((line = documentReader.readLine()) != null)
			{
				documentBuilder.append(line).append("\n");
			}

			return documentBuilder.toString();
		}
		catch (IOException e)
		{
			log.error("EXCEPTION", e);
		}
		finally
		{
			if (documentReader != null)
			{
				try
				{
					documentReader.close();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		return null;
	}

}

/*
 * $Log: av-env.bat,v $
 */