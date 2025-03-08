package net.jgp.books.spark.ch08.lab100_mysql_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;


public class MySQLToDatasetWithLongUrlApp {

    public static void main(String[] args) {
        MySQLToDatasetWithLongUrlApp app = new MySQLToDatasetWithLongUrlApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JBDC Connection")
                .master("local[*]")
                .getOrCreate();

        // Using a JDBC URL
        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila"
                + "?user=root"
                + "&password=1987"
                + "&useSSL=false"
                + "&serverTimezone=EST";

        Dataset<Row> df = spark.read()
                .jdbc(jdbcUrl, "actor", new Properties()); // пустой список свойств все равно нужен!

        df = df.orderBy(df.col("last_name"));

        System.out.println("***************************** OUTPUT *****************************");
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }

    /*
    ***************************** OUTPUT *****************************
    +--------+----------+---------+-------------------+
    |actor_id|first_name|last_name|        last_update|
    +--------+----------+---------+-------------------+
    |      92|   KIRSTEN|   AKROYD|2006-02-15 12:34:33|
    |      58| CHRISTIAN|   AKROYD|2006-02-15 12:34:33|
    |     182|    DEBBIE|   AKROYD|2006-02-15 12:34:33|
    |     118|      CUBA|    ALLEN|2006-02-15 12:34:33|
    |     145|       KIM|    ALLEN|2006-02-15 12:34:33|
    +--------+----------+---------+-------------------+
    only showing top 5 rows

    root
     |-- actor_id: integer (nullable = true)
     |-- first_name: string (nullable = true)
     |-- last_name: string (nullable = true)
     |-- last_update: timestamp (nullable = true)

    The dataframe contains 200 record(s).
     */
}
