package com.falmeida.tech;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This class uses a simple RDD from a Java List
 * @author falmeida
 *
 */
public class RDDSample {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
	
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(15.9);
		inputData.add(23.7);
		inputData.add(34.78);
		inputData.add(12.78);
		inputData.add(45.89);
		inputData.add(36.90);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample")
										.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Double> rddSample = sc.parallelize(inputData);
		
		sc.close();
		
	}
	
}
