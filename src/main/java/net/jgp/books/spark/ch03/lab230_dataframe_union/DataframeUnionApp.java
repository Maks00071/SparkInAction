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
        DataframeUnionApp app = new DataframeUnionApp();
        app.start();
    }

    /**
     * The processing code
     *
     * {1} - создание dataframe, содержащего данные о ресторанах округа Wake
     * {2} - создание dataframe, содержащего данные о ресторанах округа Durham
     * {3} - объединение двух dataframes с использованием операции, аналогичной "union" в SQL
     */
    private void start() {

        this.spark = SparkSession.builder()
                .appName("Union of two dataframes")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> wakeRestaurantsDf = buildWakeRestaurantsDataframe();          //{1}
        Dataset<Row> durhamRestaurantsDf = buildDurhamRestaurantsDataframe();      //{2}
        combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);        //{3}
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

    /**
     * Метод объединяет два dataframe df1 и df2 с помощью операции "union"
     * {1} - объединяем dataframes
     * {2} - общий счетчик записей в двух объединяемых наборах
     * {3} - смотрим сколько получилось разделов (будет два, т.к. существовало два набора данных)
     *
     * @param df1 первый dataframe
     * @param df2 второй dataframe
     */
    private void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {

        Dataset<Row> df = df1.unionByName(df2);                     //{1}

        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");        //{2}

        Partition[] partitions = df.rdd().getPartitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count: " + partitionCount);         //{3}
    }

    /*
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------+-------------------+
    |  datasetId|                name|            address1|address2|       city|state|       zip|           tel|           dateStart|             type|        geoX|       geoY|country|dateEnd|                 id|
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------+-------------------+
    |04092016024|                WABA|2502 1/2 HILLSBOR...|    NULL|    RALEIGH|   NC|     27607|(919) 833-1710|2011-10-18T00:00:...|       Restaurant|-78.66818477|35.78783803|   Wake|   NULL|NC_Wake_04092016024|
    |04092021693|  WALMART DELI #2247|2010 KILDAIRE FAR...|    NULL|       CARY|   NC|     27518|(919) 852-6651|2011-11-08T00:00:...|       Food Stand|-78.78211173|35.73717591|   Wake|   NULL|NC_Wake_04092021693|
    |04092017012|CAROLINA SUSHI &a...|5951-107 POYNER V...|    NULL|    RALEIGH|   NC|     27616|(919) 981-5835|2015-08-28T00:00:...|       Restaurant|-78.57030208|35.86511564|   Wake|   NULL|NC_Wake_04092017012|
    |04092030288|THE CORNER VENEZU...|    7500 RAMBLE WAY |    NULL|    RALEIGH|   NC|     27616|          NULL|2015-09-04T00:00:...|Mobile Food Units|  -78.537511|35.87630712|   Wake|   NULL|NC_Wake_04092030288|
    |04092015530|        SUBWAY #3726| 12233 CAPITAL BLVD |    NULL|WAKE FOREST|   NC|27587-6200|(919) 556-8266|2009-12-11T00:00:...|       Restaurant|-78.54097555|35.98087357|   Wake|   NULL|NC_Wake_04092015530|
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------+-------------------+
    only showing top 5 rows

    root
     |-- datasetId: string (nullable = true)
     |-- name: string (nullable = true)
     |-- address1: string (nullable = true)
     |-- address2: string (nullable = true)
     |-- city: string (nullable = true)
     |-- state: string (nullable = true)
     |-- zip: string (nullable = true)
     |-- tel: string (nullable = true)
     |-- dateStart: string (nullable = true)
     |-- type: string (nullable = true)
     |-- geoX: string (nullable = true)
     |-- geoY: string (nullable = true)
     |-- country: string (nullable = false)
     |-- dateEnd: string (nullable = true)
     |-- id: string (nullable = true)

    We have 5903 records.
    Partition count: 2
     */
}


























































