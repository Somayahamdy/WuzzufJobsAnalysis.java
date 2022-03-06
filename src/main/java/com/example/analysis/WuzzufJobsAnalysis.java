package com.example.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import com.example.dataAcess.WuzzufJobs;
import com.example.dataAcess.DataDAO;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.*;
import static org.apache.spark.sql.functions.col;


public class WuzzufJobsAnalysis {
    Dataset<Row> wuzzufData;
    SparkSession spark;

    public WuzzufJobsAnalysis() {
        spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

    }

    public void readData() {
        DataDAO loader = new WuzzufJobs();
        wuzzufData = loader.load("src/main/resources/Wuzzuf_Jobs.csv");
        System.out.println("------------------Wuzzuf Data Set --------------------------");
        wuzzufData.show();

        System.out.println("---------------------------Summary-------------------------------------");
        wuzzufData.summary().show();
        System.out.println("----------------Describe Wuzzuf DataSet----------------------------------");
        wuzzufData.printSchema();
        System.out.println("-------------------Most Popular Titles -------------------------------");
        Dataset<Row> MostTitles =MostPopularTitles(wuzzufData);
        MostTitles.show();
        JobTitlesBarGraph(wuzzufData);
        System.out.println("---------------------Most Popular Area -------------------------");
        Dataset<Row> MostAreas = MostPopularAreas(wuzzufData);
        MostAreas.show();
        AreasCountBarGraph(wuzzufData);






    }



    public Dataset<Row> most_popular(Dataset<Row> df, String ColName){
        return df.groupBy(ColName).count().sort(col("count").desc());
    }

    public Dataset<Row> MostPopularTitles(Dataset<Row> df)
    {

        return most_popular(df,"Title");
    }
    public Dataset<Row> MostPopularAreas(Dataset<Row> df)
    {
        return most_popular(df,"Location");
    }

    public void DrawBarChart(Dataset<Row> df,String Xcol,String Ycol,String titleName, String xAxisTitle,String yAxisTitle,String SeriesName)
    {
        Dataset<Row>  Popular_df = df.limit(5);
        List<String> Col_Selection = Popular_df.select(Xcol).as(Encoders.STRING()).collectAsList() ;
        List<Long> counts = Popular_df .select(Ycol).as(Encoders.LONG()).collectAsList();

        CategoryChart chart = new CategoryChartBuilder().width (900).height (600).title (titleName).xAxisTitle (xAxisTitle).yAxisTitle (yAxisTitle).build ();
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideN);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);

        chart.addSeries (SeriesName, Col_Selection, counts);

        new SwingWrapper(chart).displayChart ();
    }

    public void JobTitlesBarGraph(Dataset<Row> df)
    {
        Dataset<Row> MostTitles_df= MostPopularTitles( df);
        DrawBarChart(MostTitles_df,"Title","count","Most Popular Title","Titles","Count","Titles's Count");

    }
    public void AreasCountBarGraph(Dataset<Row> df)
    {
        Dataset<Row> MostAreas_df= MostPopularAreas( df);
        DrawBarChart(MostAreas_df,"Location","count","Most Popular Areas","Areas","Count","Area's Count");

    }

}