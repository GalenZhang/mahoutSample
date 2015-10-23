package com.achievo.hadoop.storm.trident.tfidf.functions;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: TermFilter.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TermFilter.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 19, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TermFilter extends BaseFunction
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 8390251934362840554L;

	private SpellChecker spellChecker;

	private List<String> filterTerms = Arrays.asList(new String[] {"http"});

	private Logger log = LoggerFactory.getLogger(TermFilter.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context)
	{
		super.prepare(conf, context);
		String dir = System.getProperty("user.home") + "/dictionaries";
		Directory directory;

		try
		{
			directory = FSDirectory.open(Paths.get(dir));
			spellChecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			URL dictionaryFile = TermFilter.class.getResource("/dictionaries/fulldictionary00.txt");
			spellChecker.indexDictionary(new PlainTextDictionary(Paths.get(dictionaryFile.toURI())), config, true);
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}
	}

	public boolean isKeep(TridentTuple tuple)
	{
		log.debug("Filtering tuple");

		return shouldKeep(tuple.getString(0));
	}

	private boolean shouldKeep(String stem)
	{
		if (stem == null || "".equals(stem))
		{
			return false;
		}

		if (filterTerms.contains(stem))
		{
			return false;
		}

		try
		{
			Integer.parseInt(stem);
			return false;
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}

		try
		{
			Double.parseDouble(stem);
			return false;
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}

		try
		{
			return spellChecker.exist(stem);
		}
		catch (Exception e)
		{
			log.error("EXCEPTION", e);
		}
		return false;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector)
	{
		if (isKeep(tuple))
		{
			collector.emit(tuple);
		}
	}

}

/*
 * $Log: av-env.bat,v $
 */