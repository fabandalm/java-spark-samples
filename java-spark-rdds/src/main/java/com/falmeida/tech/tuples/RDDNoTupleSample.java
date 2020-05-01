package com.falmeida.tech.tuples;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDNoTupleSample {

	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<Integer>();
		inputData.add(25);
		inputData.add(64);
		inputData.add(16);
		inputData.add(4);
		inputData.add(256);
		inputData.add(1024);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> rddSample = sc.parallelize(inputData);
		
		JavaRDD<IntegerSqrt> rddIntegerSqrt = rddSample.map(value -> new IntegerSqrt(value));
		
		rddIntegerSqrt.collect().forEach(System.out::println);
		
		sc.close();
		
	}
	
}
