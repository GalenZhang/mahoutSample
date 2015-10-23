package com.achievo.hadoop.storm.trident.tfidf.functions;

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
 *  File: TfidfExpression.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TfidfExpression.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 19, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TfidfExpression extends BaseFunction
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -5203959877803785869L;

	private Logger log = LoggerFactory.getLogger(TfidfExpression.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector)
	{
		try
		{
			double d = (double) tuple.getLongByField("d");
			double df = (double) tuple.getLongByField("df");
			double tf = (double) tuple.getLongByField("tf");

			log.debug(String.format("d=%f;df=%f;tf=%f", d, df, tf));

			double tfidf = tf * Math.log(d / (1 + df));
			log.debug(String.format("Emitting new TFIDF(term, Document): (%s, %s) = %f",
				tuple.getStringByField("term"), tuple.getStringByField("documentId"), tfidf));

			collector.emit(new Values(tfidf));
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */