package net.jgp.books.spark.ch08.lab100_mysql_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class MySQLToDatasetWithOptionApp {

    public static void main(String[] args) {
        MySQLToDatasetWithOptionApp app = new MySQLToDatasetWithOptionApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JBDC Connection")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("url", "jdbc:mysql://localhost:3306/sakila")
                .option("dbtable", "actor")
                .option("user", "root")
                .option("password", "1987")
                .option("useSSL", "false")
                .option("serverTimezone", "EST")
                .format("jdbc")
                .load();

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
