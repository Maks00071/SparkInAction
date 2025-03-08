package net.jgp.books.spark.ch08.lab300_advanced_queries;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;


public class MySQLWithWhereClauseToDatasetApp {

    public static void main(String[] args) {
        MySQLWithWhereClauseToDatasetApp app = new MySQLWithWhereClauseToDatasetApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL with where clause to Dataframe using a JDBC Connection")
                .master("local[*]")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "1987");
        props.put("useSSL", "false");
        props.put("serverTimezone", "EST");

        String sqlQuery = "select * from film where "
                + "(title like \"%ALIEN%\" or title like \"%victory%\" "
                + "or title like \"%agent%\" or description like \"%action%\") "
                + "and rental_rate>1 "
                + "and (rating=\"G\" or rating=\"PG\")";

        Dataset<Row> df = spark.read()
                .jdbc(
                        "jdbc:mysql://localhost:3306/sakila",
                        "(" + sqlQuery + ") filf_alias",
                        props);

        System.out.println("***************************** OUTPUT *****************************");
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }

    /*
    ***************************** OUTPUT *****************************
    +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
    |film_id|         title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
    +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
    |      6|  AGENT TRUMAN|A Intrepid Panora...|  2006-01-01|          1|                null|              3|       2.99|   169|           17.99|    PG|      Deleted Scenes|2006-02-15 13:03:42|
    |     13|   ALI FOREVER|A Action-Packed D...|  2006-01-01|          1|                null|              4|       4.99|   150|           21.99|    PG|Deleted Scenes,Be...|2006-02-15 13:03:42|
    |    137|CHARADE DUFFEL|A Action-Packed D...|  2006-01-01|          1|                null|              3|       2.99|    66|           21.99|    PG|Trailers,Deleted ...|2006-02-15 13:03:42|
    |    217|    DAZED PUNK|A Action-Packed S...|  2006-01-01|          1|                null|              6|       4.99|   120|           20.99|     G|Commentaries,Dele...|2006-02-15 13:03:42|
    |    396|  HANGING DEEP|A Action-Packed Y...|  2006-01-01|          1|                null|              5|       4.99|    62|           18.99|     G|Trailers,Commenta...|2006-02-15 13:03:42|
    +-------+--------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
    only showing top 5 rows

    root
     |-- film_id: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- description: string (nullable = true)
     |-- release_year: date (nullable = true)
     |-- language_id: integer (nullable = true)
     |-- original_language_id: integer (nullable = true)
     |-- rental_duration: integer (nullable = true)
     |-- rental_rate: decimal(4,2) (nullable = true)
     |-- length: integer (nullable = true)
     |-- replacement_cost: decimal(5,2) (nullable = true)
     |-- rating: string (nullable = true)
     |-- special_features: string (nullable = true)
     |-- last_update: timestamp (nullable = true)

    The dataframe contains 16 record(s).
     */
}
