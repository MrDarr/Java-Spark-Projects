package com.RUSpark;

/* any necessary Java packages here */
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
public class RedditHourImpact {
	public static final String punctuation = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
    String inputPath = args[0];

    SparkSession spark = SparkSession.builder()
            .appName("RedditHourImpact")
            .getOrCreate();

    JavaRDD<String> lines = spark.read()
            .textFile(inputPath)
            .javaRDD();

    JavaPairRDD<Integer, Integer> indiv = lines
            .mapToPair(s -> {
                String[] parts = s.split(punctuation);
                int hour = ((Integer.parseInt(parts[1]) - 18000) / 3600) % 24;
                int sum = Integer.parseInt(parts[4]) + Integer.parseInt(parts[5]) + Integer.parseInt(parts[6]);
                return new Tuple2<>(hour, sum);
            });

    JavaPairRDD<Integer, Integer> counts = indiv.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<Integer, Integer>> output = counts.collect();

		for (Tuple2<Integer, Integer> tuple : output) {
			System.out.println(tuple._1() + " " + tuple._2());
		}
		
		spark.stop();
	}

}
