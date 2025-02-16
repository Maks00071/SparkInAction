package net.jgp.books.spark.ch03.lab230_dataframe_union;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Union of two dataframes.
 *
 * @author jgp
 */
public class DataframeUnionApp {

    private SparkSession spark;

    public static void main(String[] args) {

    }

    /**
     * The processing code
     */
    private void start() {

        this.spark = SparkSession.builder()
                .appName("Union of two dataframes")
                .master("local[*]")
                .getOrCreate();


    }

    /**
     * Метод загружает файл "Restaurants_in_Wake_County_NC.csv" из источника,
     * формирует из него целевой dataframe в соответствии с предъявляемыми требованиями
     *
     * @return Dataset<Row> df
     */
    private Dataset<Row> buildWakeRestaurantsDataframe() {

        Dataset<Row> df = this.spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/Lesson3/Restaurants_in_Wake_County_NC.csv");

        //формируем целевой dataframe для "Restaurants_in_Wake_County_NC"
        df = df.withColumn("country", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumn("dateEnd", lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop(df.col("OBJECTID"))
                .drop(df.col("GEOCODESTATUS"))
                .drop(df.col("PERMITID"));

        //добавим уникальный идентификатор
        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("country"), lit("_"),
                df.col("datasetId")));

        return df;
    }

    /**
     * Метод загружает файл из источника "Restaurants_in_Durham_County_NC.json",
     * формирует из него целевой dataframe в соответствии с предъявляемыми требованиями
     *
     * @return
     */
    private Dataset<Row> buildDurhamRestaurantsDataframe() {

        Dataset<Row> df = this.spark.read()
                .format("json")
                .load("data/Lesson3/Restaurants_in_Durham_County_NC.json");

        //формируем целевой dataframe для "Restaurants_in_Durham_County_NC"
        df = df.withColumn("country", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type",
                        split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1))
                .drop(df.col("fields"))
                .drop(df.col("geometry"))
                .drop(df.col("record_timestamp"))
                .drop(df.col("recordid"));

        //добавим уникальный идентификатор
        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("country"), lit("_"),
                df.col("datasetId")));

        return df;
    }
}


























































