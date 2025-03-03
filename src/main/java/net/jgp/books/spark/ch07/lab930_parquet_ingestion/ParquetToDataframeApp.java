package net.jgp.books.spark.ch07.lab930_parquet_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ParquetToDataframeApp {

    public static void main(String[] args) {
        ParquetToDataframeApp app = new ParquetToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Parquet to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("parquet")
                .load("data/ch07/alltypes_plain.parquet");

        System.out.println("********************** OUTPUT **********************");
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " row");
    }

    /*
    ********************** OUTPUT **********************
    +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
    | id|bool_col|tinyint_col|smallint_col|int_col|bigint_col|float_col|double_col|     date_string_col|string_col|      timestamp_col|
    +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
    |  4|    true|          0|           0|      0|         0|      0.0|       0.0|[30 33 2F 30 31 2...|      [30]|2009-03-01 03:00:00|
    |  5|   false|          1|           1|      1|        10|      1.1|      10.1|[30 33 2F 30 31 2...|      [31]|2009-03-01 03:01:00|
    |  6|    true|          0|           0|      0|         0|      0.0|       0.0|[30 34 2F 30 31 2...|      [30]|2009-04-01 04:00:00|
    |  7|   false|          1|           1|      1|        10|      1.1|      10.1|[30 34 2F 30 31 2...|      [31]|2009-04-01 04:01:00|
    |  2|    true|          0|           0|      0|         0|      0.0|       0.0|[30 32 2F 30 31 2...|      [30]|2009-02-01 03:00:00|
    |  3|   false|          1|           1|      1|        10|      1.1|      10.1|[30 32 2F 30 31 2...|      [31]|2009-02-01 03:01:00|
    |  0|    true|          0|           0|      0|         0|      0.0|       0.0|[30 31 2F 30 31 2...|      [30]|2009-01-01 03:00:00|
    |  1|   false|          1|           1|      1|        10|      1.1|      10.1|[30 31 2F 30 31 2...|      [31]|2009-01-01 03:01:00|
    +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+

    root
     |-- id: integer (nullable = true)
     |-- bool_col: boolean (nullable = true)
     |-- tinyint_col: integer (nullable = true)
     |-- smallint_col: integer (nullable = true)
     |-- int_col: integer (nullable = true)
     |-- bigint_col: long (nullable = true)
     |-- float_col: float (nullable = true)
     |-- double_col: double (nullable = true)
     |-- date_string_col: binary (nullable = true)
     |-- string_col: binary (nullable = true)
     |-- timestamp_col: timestamp (nullable = true)

    The dataframe has 8 row
     */
}
