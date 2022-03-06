package com.example.SparkController;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

public class controller {

    @Controller
    public class SparkController {
        @Autowired
        private SparkSession sparkSession;


    }


}
