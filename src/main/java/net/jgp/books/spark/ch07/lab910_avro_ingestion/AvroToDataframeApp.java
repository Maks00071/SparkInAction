package net.jgp.books.spark.ch07.lab910_avro_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class AvroToDataframeApp {

    public static void main(String[] args) {
        AvroToDataframeApp app = new AvroToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Avro to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("avro")
                .load("data/ch07/weather.avro");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows");

    }

    /*
    +------------+-------------+----+
    |     station|         time|temp|
    +------------+-------------+----+
    |011990-99999|-619524000000|   0|
    |011990-99999|-619506000000|  22|
    |011990-99999|-619484400000| -11|
    |012650-99999|-655531200000| 111|
    |012650-99999|-655509600000|  78|
    +------------+-------------+----+

    root
     |-- station: string (nullable = true)
     |-- time: long (nullable = true)
     |-- temp: integer (nullable = true)

    The dataframe has 5 rows
     */
}
