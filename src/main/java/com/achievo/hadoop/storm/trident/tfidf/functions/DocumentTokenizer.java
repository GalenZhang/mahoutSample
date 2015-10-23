package com.achievo.hadoop.storm.trident.tfidf.functions;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.trident.tfidf.common.TfidfTopologyFields;

import edu.washington.cs.knowitall.morpha.MorphaStemmer;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: DocumentTokenizer.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: DocumentTokenizer.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class DocumentTokenizer extends BaseFunction
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -384365645350404901L;

	private Logger log = LoggerFactory.getLogger(DocumentTokenizer.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector)
	{
		String documentContents = tuple.getStringByField(TfidfTopologyFields.DOCUMENT);
		TokenStream ts = null;

		try
		{
			Tokenizer tokenStream = new StandardTokenizer();
			tokenStream.setReader(new StringReader(documentContents));

			ts = new StopFilter(tokenStream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
			CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
			while (ts.incrementToken())
			{
				String lemma = MorphaStemmer.stemToken(termAtt.toString());
				lemma = lemma.trim().replace("\n", "").replace("\r", "");
				collector.emit(new Values(lemma));
			}
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}
		finally
		{
			if (ts != null)
			{
				try
				{
					ts.close();
				}
				catch (IOException e)
				{
					log.error(e.toString());
				}
			}
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */