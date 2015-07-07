package com.achievo.mahout.recommendation.book;

import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: BookResult.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: BookResult.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 7, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class BookResult
{
	final static int NEIGHBORHOOD_NUM = 2;

	final static int RECOMMENDER_NUM = 3;

	public static void main(String[] args) throws IOException, TasteException
	{
		String file = BookResult.class.getClassLoader().getResource("").getPath() + "datafile/book/rating.csv";
		DataModel dataModel = RecommendFactory.buildDataModel(file);
		RecommenderBuilder rb1 = BookEvaluator.userEuclidean(dataModel);
		RecommenderBuilder rb2 = BookEvaluator.itemEuclidean(dataModel);
		RecommenderBuilder rb3 = BookEvaluator.userEuclideanNoPref(dataModel);
		RecommenderBuilder rb4 = BookEvaluator.itemEuclideanNoPref(dataModel);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext())
		{
			long uid = iter.nextLong();
			System.out.println("userEuclidean    => ");
			result(uid, rb1, dataModel);
			System.out.println("itemEuclidean    => ");
			result(uid, rb2, dataModel);
			System.out.println("userEuclideanNoPref    => ");
			result(uid, rb3, dataModel);
			System.out.println("itemEuclideanNoPref    => ");
			result(uid, rb4, dataModel);
		}
	}

	public static void result(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel)
			throws TasteException
	{
		List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
		RecommendFactory.showItems(uid, list, false);
	}
}

/*
 * $Log: av-env.bat,v $
 */