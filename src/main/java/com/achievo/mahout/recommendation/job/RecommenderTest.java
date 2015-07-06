package com.achievo.mahout.recommendation.job;

import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: RecommenderTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: RecommenderTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 3, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class RecommenderTest
{
	final static int NEIGHBORHOOD_NUM = 2;

	final static int RECOMMENDER_NUM = 3;

	public static void main(String[] args) throws TasteException, IOException
	{
		RandomUtils.useTestSeed();
		String file = RecommenderTest.class.getClassLoader().getResource("").getPath() + "datafile/item.csv";
		DataModel dataModel = RecommendFactory.buildDataModel(file);
		// slopeOne(dataModel);
		userCF(dataModel);
	}

	public static void userCF(DataModel dataModel) throws TasteException
	{
		UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST,
			userSimilarity, dataModel, NEIGHBORHOOD_NUM);
		RecommenderBuilder recommenderBuilder = RecommendFactory
				.userRecommender(userSimilarity, userNeighborhood, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext())
		{
			long uid = iter.nextLong();
			List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
			RecommendFactory.showItems(uid, list, true);
		}
	}

	public static void itemCF(DataModel dataModel) throws TasteException
	{
		ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext())
		{
			long uid = iter.nextLong();
			List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
			RecommendFactory.showItems(uid, list, true);
		}
	}

	public static void slopeOne(DataModel dataModel) throws TasteException
	{
	}
}

/*
 * $Log: av-env.bat,v $
 */