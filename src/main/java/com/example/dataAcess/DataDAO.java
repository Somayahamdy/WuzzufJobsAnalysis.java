package com.example.dataAcess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataDAO {
    public Dataset<Row> load(String fileName);

}
