package com.achievo.mahout.recommendation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.mahout.clustering.UncommonDistributions;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: MathUtil.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://github.com/bsspirit/maven_mahout_template/blob/mahout-0.6/src/main/java/org/conan/mymahout/cluster06/MathUtil.java
 * 
 *  Notes:
 * 	$Id: MathUtil.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class MathUtil
{
	public static List<Vector> readFileToVector(String file) throws IOException
	{
		List<Vector> vectors = new ArrayList<Vector>();
		BufferedReader buffer = new BufferedReader(new FileReader(file));
		String line = null;
		while ((line = buffer.readLine()) != null)
		{
			String[] arr = line.split(",");
			vectors.add(new DenseVector(new double[] {Double.parseDouble(arr[0]), Double.parseDouble(arr[1])}));
		}
		buffer.close();
		return vectors;
	}

	public static void generateSamples(List<Vector> vectors, int num, double mx, double my, double sd)
	{
		for (int i = 0; i < num; i++)
		{
			vectors.add(new DenseVector(new double[] {UncommonDistributions.rNorm(mx, sd),
					UncommonDistributions.rNorm(my, sd)}));
		}
	}

	public static List<Vector> chooseRandomPoints(Iterable<Vector> vectors, int k)
	{
		List<Vector> chosenPoints = new ArrayList<Vector>(k);
		Random random = RandomUtils.getRandom();
		for (Vector value : vectors)
		{
			int currentSize = chosenPoints.size();
			if (currentSize < k)
			{
				chosenPoints.add(value);
			}
			else if (random.nextInt(currentSize + 1) == 0)
			{
				int indexToRemove = random.nextInt(currentSize);
				chosenPoints.remove(indexToRemove);
				chosenPoints.add(value);
			}
		}
		return chosenPoints;
	}

	public static void main(String[] args) throws IOException
	{
		readFileToVector(MathUtil.class.getClassLoader().getResource("").getPath() + "datafile/randomData.csv");
	}
}

/*
 * $Log: av-env.bat,v $
 */