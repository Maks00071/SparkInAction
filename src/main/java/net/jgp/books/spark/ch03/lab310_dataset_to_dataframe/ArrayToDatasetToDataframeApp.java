package net.jgp.books.spark.ch03.lab310_dataset_to_dataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class ArrayToDatasetToDataframeApp {

    public static void main(String[] args) {
        ArrayToDatasetToDataframeApp app = new ArrayToDatasetToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Array to dataframe")
                .master("local[*]")
                .getOrCreate();

        String[] stringList = new String[] {"Jean", "Liz", "Pierre", "Lauric"};
        List<String> data = Arrays.asList(stringList);

        System.out.println("************************ Dataset<String> **********************************");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

        System.out.println("************************ Dataset<Row> **********************************");

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();

    }

    /*
    ************************ Dataset<String> **********************************
    +------+
    | value|
    +------+
    |  Jean|
    |   Liz|
    |Pierre|
    |Lauric|
    +------+

    root
     |-- value: string (nullable = true)

    ************************ Dataset<Row> **********************************
    +------+
    | value|
    +------+
    |  Jean|
    |   Liz|
    |Pierre|
    |Lauric|
    +------+

    root
     |-- value: string (nullable = true)
     */
}



























