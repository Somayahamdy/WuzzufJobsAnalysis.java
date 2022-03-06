package com.example.main;

import com.example.analysis.WuzzufJobsAnalysis;

import org.apache.spark.sql.SparkSession;
// import org.springframework.boot.autoconfigure.SpringBootApplication;

// @SpringBootApplication
public class WuzzufJobsAnalysisApplication {

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark ML project")
				.master("local[1]")
				.config("spark.master", "local")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		(new WuzzufJobsAnalysis()).readData();


	}

}
