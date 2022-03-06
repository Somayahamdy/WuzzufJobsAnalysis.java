package com.example.dataAcess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WuzzufJobs implements DataDAO{
    public WuzzufJobs() {
        super();
    }
    public Dataset<Row> load(String filename) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .csv(filename);

        return dataset;
    }
}
