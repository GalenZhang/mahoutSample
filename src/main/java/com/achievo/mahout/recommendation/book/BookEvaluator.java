package com.achievo.mahout.recommendation.book;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: BookEvaluator.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.fens.me/hadoop-mahout-recommend-book/
 * 
 *  Notes:
 * 	$Id: BookEvaluator.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 7, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class BookEvaluator
{
	final static int NEIGHBORHOOD_NUM = 2;

	final static int RECOMMENDER_NUM = 3;

	public static void main(String[] args) throws TasteException, IOException
	{
		String file = BookEvaluator.class.getClassLoader().getResource("").getPath() + "datafile/book/rating.csv";
		DataModel dataModel = RecommendFactory.buildDataModel(file);
		userEuclidean(dataModel);
		userLoglikelihood(dataModel);
		userEuclideanNoPref(dataModel);
		itemEuclidean(dataModel);
		itemLoglikelihood(dataModel);
		itemEuclideanNoPref(dataModel);
		// slopeOne(dataModel);
	}

	public static RecommenderBuilder itemEuclideanNoPref(DataModel dataModel) throws TasteException
	{
		System.out.println("itemLoglikelihood");
		ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, false);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;
	}

	public static RecommenderBuilder itemLoglikelihood(DataModel dataModel) throws TasteException
	{
		System.out.println("itemLoglikelihood");
		ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD,
			dataModel);
		RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;
	}

	public static RecommenderBuilder itemEuclidean(DataModel dataModel) throws TasteException
	{
		System.out.println("itemEuclidean");
		ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;
	}

	public static RecommenderBuilder userEuclideanNoPref(DataModel dataModel) throws TasteException
	{
		System.out.println("userEuclideanNoPref");
		UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST,
			userSimilarity, dataModel, NEIGHBORHOOD_NUM);
		RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood,
			false);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;

	}

	public static RecommenderBuilder userLoglikelihood(DataModel dataModel) throws TasteException
	{
		System.out.println("userLoglikelihood");
		UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD,
			dataModel);
		UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST,
			userSimilarity, dataModel, NEIGHBORHOOD_NUM);
		RecommenderBuilder recommenderBuilder = RecommendFactory
				.userRecommender(userSimilarity, userNeighborhood, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;
	}

	public static RecommenderBuilder userEuclidean(DataModel dataModel) throws TasteException
	{
		System.out.println("userEuclidean");
		UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN,
			dataModel);
		UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST,
			userSimilarity, dataModel, NEIGHBORHOOD_NUM);
		RecommenderBuilder recommenderBuilder = RecommendFactory
				.userRecommender(userSimilarity, userNeighborhood, true);

		RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
			dataModel, 0.7);
		RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
		return recommenderBuilder;
	}

	// public static RecommenderBuilder slopeOne(DataModel dataModel) throws TasteException, IOException
	// {
	// System.out.println("slopeOne");
	// RecommenderBuilder recommenderBuilder = RecommendFactory.slopeOneRecommender();
	//
	// RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
	// dataModel, 0.7);
	// RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
	// return recommenderBuilder;
	// }
}

/*
 * $Log: av-env.bat,v $
 */