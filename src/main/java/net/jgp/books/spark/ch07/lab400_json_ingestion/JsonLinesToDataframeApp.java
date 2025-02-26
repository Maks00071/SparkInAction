package net.jgp.books.spark.ch07.lab400_json_ingestion;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class JsonLinesToDataframeApp {

    public static void main(String[] args) {
        JsonLinesToDataframeApp app = new JsonLinesToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/ch07/durham-nc-foreclosure-2006-2016.json");

        df.show(5, 20);

        df.printSchema();

//        Dataset<Row> testDf = spark.read().json("data/ch07/durham-nc-foreclosure-2006-2016.json");
//        testDf.printSchema();
//        testDf.show(5, 50);
//
//        Dataset<Row> df2 =spark.read()
//                .json(sc.wholeTextFiles("data/ch07/durham-nc-foreclosure-2006-2016.json").map(t -> t._2()));
//        df2.printSchema();
//        df2.show(5, 50);
    }

    /*
    with option("multiline", false)
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |           datasetid|              fields|            geometry|    record_timestamp|            recordid|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |foreclosure-2006-...|{217 E CORPORATIO...|{[-78.8922549, 36...|2017-03-06T12:41:...|629979c85b1cc68c1...|
    |foreclosure-2006-...|{401 N QUEEN ST, ...|{[-78.895396, 35....|2017-03-06T12:41:...|e3cce8bbc3c9b804c...|
    |foreclosure-2006-...|{403 N QUEEN ST, ...|{[-78.8950321, 35...|2017-03-06T12:41:...|311559ebfeffe7ebc...|
    |foreclosure-2006-...|{918 GILBERT ST, ...|{[-78.8873774, 35...|2017-03-06T12:41:...|7ec0761bd385bab8a...|
    |foreclosure-2006-...|{721 LIBERTY ST, ...|{[-78.888343, 35....|2017-03-06T12:41:...|c81ae2921ffca8125...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 5 rows

    root
     |-- datasetid: string (nullable = true)
     |-- fields: struct (nullable = true)
     |    |-- address: string (nullable = true)
     |    |-- geocode: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- parcel_number: string (nullable = true)
     |    |-- year: string (nullable = true)
     |-- geometry: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- record_timestamp: string (nullable = true)
     |-- recordid: string (nullable = true)
     */
}
