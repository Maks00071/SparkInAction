package net.jgp.books.spark.ch08.lab310_sql_joins;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;


public class MySQLWithJoinToDatasetApp {

    public static void main(String[] args) {
        MySQLWithJoinToDatasetApp app = new MySQLWithJoinToDatasetApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL with join to Dataframe using JDBC")
                .master("local[*]")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "1987");
        props.put("useSSL", "false");
        props.put("serverTimezone", "EST");

        // Builds the SQL query doing the join operation
        String sqlQuery = "select actor.first_name, actor.last_name, film.title, "
                + "film.description "
                + "from actor, film_actor, film "
                + "where actor.actor_id = film.film_id";

        Dataset<Row> df = spark.read()
                .jdbc(
                        "jdbc:mysql://localhost:3306/sakila",
                        "(" + sqlQuery + ") actor_film_alias",
                        props);

        System.out.println("***************************** OUTPUT *****************************");
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }

    /*
    ***************************** OUTPUT *****************************
    +----------+---------+------------------+--------------------+
    |first_name|last_name|             title|         description|
    +----------+---------+------------------+--------------------+
    |     THORA|   TEMPLE| CURTAIN VIDEOTAPE|A Boring Reflecti...|
    |     JULIA|  FAWCETT|  CUPBOARD SINNERS|A Emotional Refle...|
    |      MARY|   KEITEL|  CRYSTAL BREAKING|A Fast-Paced Char...|
    |     REESE|     WEST|     CRUSADE HONEY|A Fast-Paced Refl...|
    |      BELA|   WALKEN|CRUELTY UNFORGIVEN|A Brilliant Tale ...|
    +----------+---------+------------------+--------------------+
    only showing top 5 rows

    root
     |-- first_name: string (nullable = true)
     |-- last_name: string (nullable = true)
     |-- title: string (nullable = true)
     |-- description: string (nullable = true)

    The dataframe contains 1092400 record(s).
     */
}




















