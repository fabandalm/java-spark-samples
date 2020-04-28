package com.falmeida.tech.reduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDReduceSample {

	public static void main(String[] args) {
		
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(15.00);
		inputData.add(20.00);
		inputData.add(25.00);
		inputData.add(40.00);
		inputData.add(50.00);
		inputData.add(100.00);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample")
										.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Double> rddSample = sc.parallelize(inputData);
		
		Double result = rddSample.reduce((value1,value2) -> value1 + value2);
		
		System.out.println(result);
		
		sc.close();
		
	}
	
}
