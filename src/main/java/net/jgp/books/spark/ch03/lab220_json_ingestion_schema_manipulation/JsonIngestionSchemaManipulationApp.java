package net.jgp.books.spark.ch03.lab220_json_ingestion_schema_manipulation;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * CSV ingestion in a dataframe.
 *
 * @author jgp
 */
public class JsonIngestionSchemaManipulationApp {

    public static void main(String[] args) {
        JsonIngestionSchemaManipulationApp app = new JsonIngestionSchemaManipulationApp();
        app.start();
    }

    /**
     * The processing code.
     * {1} - Как и с CSV, к JSON можно добавлять столбцы с названием
     * {2} - Доступ к вложенным полям с помощью символа точки "."
     * {3} - Описание в формате <id> - <label> можно разделить по символу "." и взять
     *       элемент по индексу
     * {4} - Извлечение первого элемента массива как широты ("geoX")
     * {5} - Извлечение первого элемента массива как широты ("geoY")
     */
    private void start() {

        //Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Durham County, NC")
                .master("local[*]")
                .getOrCreate();

        // Reads a JSON file called Restaurants_in_Durham_County_NC.json, stores
        // it
        // in a dataframe
        Dataset<Row> df = spark.read()
                .format("json")
                .load("data/Lesson3/Restaurants_in_Durham_County_NC.json");

        System.out.println("*** Right after ingestion: ");
        df.show(5);
        df.printSchema();

        System.out.println("We have " + df.count() + " records.");

        df = df.withColumn("country", lit("Durham"))                                                    //{1}
                .withColumn("datasetId", df.col("fields.id"))                                             //{2}
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1)) //"type_description": "1 - Restaurant"  //{3}
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))  //"geolocation": [36.0467802, -78.8895483]  //{4}
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));                    //{5}

        df = df.withColumn("id",
                concat(df.col("state"), lit("_"),
                        df.col("country"), lit("_"),
                        df.col("datasetId")));

        System.out.println("*** Dataframe transformed");
        df.show(5);
        df.printSchema();

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().getPartitions();
        int partitionsCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionsCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " + df.rdd().getPartitions().length);

    }

    /*
    *** Right after ingestion:
    +----------------+--------------------+--------------------+--------------------+--------------------+
    |       datasetid|              fields|            geometry|    record_timestamp|            recordid|
    +----------------+--------------------+--------------------+--------------------+--------------------+
    |restaurants-data|{NULL, Full-Servi...|{[-78.9573299, 35...|2017-07-13T09:15:...|1644654b953d1802c...|
    |restaurants-data|{NULL, Nursing Ho...|{[-78.8895483, 36...|2017-07-13T09:15:...|93573dbf8c9e799d8...|
    |restaurants-data|{NULL, Fast Food ...|{[-78.9593263, 35...|2017-07-13T09:15:...|0d274200c7cef50d0...|
    |restaurants-data|{NULL, Full-Servi...|{[-78.9060312, 36...|2017-07-13T09:15:...|cf3e0b175a6ebad2a...|
    |restaurants-data|{NULL, NULL, [36....|{[-78.9135175, 36...|2017-07-13T09:15:...|e796570677f7c39cc...|
    +----------------+--------------------+--------------------+--------------------+--------------------+
     */

    /*
    root
     |-- datasetid: string (nullable = true)
     |-- fields: struct (nullable = true)
     |    |-- closing_date: string (nullable = true)
     |    |-- est_group_desc: string (nullable = true)
     |    |-- geolocation: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- hours_of_operation: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- insp_freq: long (nullable = true)
     |    |-- opening_date: string (nullable = true)
     |    |-- premise_address1: string (nullable = true)
     |    |-- premise_address2: string (nullable = true)
     |    |-- premise_city: string (nullable = true)
     |    |-- premise_name: string (nullable = true)
     |    |-- premise_phone: string (nullable = true)
     |    |-- premise_state: string (nullable = true)
     |    |-- premise_zip: string (nullable = true)
     |    |-- risk: long (nullable = true)
     |    |-- rpt_area_desc: string (nullable = true)
     |    |-- seats: long (nullable = true)
     |    |-- sewage: string (nullable = true)
     |    |-- smoking_allowed: string (nullable = true)
     |    |-- status: string (nullable = true)
     |    |-- transitional_type_desc: string (nullable = true)
     |    |-- type_description: string (nullable = true)
     |    |-- water: string (nullable = true)
     |-- geometry: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- record_timestamp: string (nullable = true)
     |-- recordid: string (nullable = true)
     */

    // We have 2463 records.

    /*
    *** Dataframe transformed
    +---------+--------------------+--------------------+--------------------+--------------------+-------+--------------------+--------------------+--------+------+-----+-----+--------------+----------+-------+--------------------+----------+-----------+---------------+
    |datasetId|              fields|            geometry|    record_timestamp|            recordid|country|                name|            address1|address2|  city|state|  zip|           tel| dateStart|dateEnd|                type|      geoX|       geoY|             id|
    +---------+--------------------+--------------------+--------------------+--------------------+-------+--------------------+--------------------+--------+------+-----+-----+--------------+----------+-------+--------------------+----------+-----------+---------------+
    |    56060|{NULL, Full-Servi...|{[-78.9573299, 35...|2017-07-13T09:15:...|1644654b953d1802c...| Durham|    WEST 94TH ST PUB| 4711 HOPE VALLEY RD|SUITE 6C|DURHAM|   NC|27707|(919) 403-0025|1994-09-01|   NULL|          Restaurant|35.9207272|-78.9573299|NC_Durham_56060|
    |    58123|{NULL, Nursing Ho...|{[-78.8895483, 36...|2017-07-13T09:15:...|93573dbf8c9e799d8...| Durham|BROOKDALE DURHAM IFS|4434 BEN FRANKLIN...|    NULL|DURHAM|   NC|27704|(919) 479-9966|2003-10-15|   NULL|Institutional Foo...|36.0467802|-78.8895483|NC_Durham_58123|
    |    70266|{NULL, Fast Food ...|{[-78.9593263, 35...|2017-07-13T09:15:...|0d274200c7cef50d0...| Durham|       SMOOTHIE KING|1125 W. NC HWY 54...|    NULL|DURHAM|   NC|27707|(919) 489-7300|2009-07-09|   NULL|          Restaurant|35.9182655|-78.9593263|NC_Durham_70266|
    |    97837|{NULL, Full-Servi...|{[-78.9060312, 36...|2017-07-13T09:15:...|cf3e0b175a6ebad2a...| Durham|HAMPTON INN & SUITES|   1542 N GREGSON ST|    NULL|DURHAM|   NC|27701|(919) 688-8880|2012-01-09|   NULL|          Restaurant|36.0183378|-78.9060312|NC_Durham_97837|
    |    60690|{NULL, NULL, [36....|{[-78.9135175, 36...|2017-07-13T09:15:...|e796570677f7c39cc...| Durham|BETTER LIVING CON...|       909 GARCIA ST|    NULL|DURHAM|   NC|27704|(919) 477-5825|2008-06-02|   NULL|    Residential Care|36.0556347|-78.9135175|NC_Durham_60690|
    +---------+--------------------+--------------------+--------------------+--------------------+-------+--------------------+--------------------+--------+------+-----+-----+--------------+----------+-------+--------------------+----------+-----------+---------------+
     */

    /*
    root
     |-- datasetId: string (nullable = true)
     |-- fields: struct (nullable = true)
     |    |-- closing_date: string (nullable = true)
     |    |-- est_group_desc: string (nullable = true)
     |    |-- geolocation: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- hours_of_operation: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- insp_freq: long (nullable = true)
     |    |-- opening_date: string (nullable = true)
     |    |-- premise_address1: string (nullable = true)
     |    |-- premise_address2: string (nullable = true)
     |    |-- premise_city: string (nullable = true)
     |    |-- premise_name: string (nullable = true)
     |    |-- premise_phone: string (nullable = true)
     |    |-- premise_state: string (nullable = true)
     |    |-- premise_zip: string (nullable = true)
     |    |-- risk: long (nullable = true)
     |    |-- rpt_area_desc: string (nullable = true)
     |    |-- seats: long (nullable = true)
     |    |-- sewage: string (nullable = true)
     |    |-- smoking_allowed: string (nullable = true)
     |    |-- status: string (nullable = true)
     |    |-- transitional_type_desc: string (nullable = true)
     |    |-- type_description: string (nullable = true)
     |    |-- water: string (nullable = true)
     |-- geometry: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- record_timestamp: string (nullable = true)
     |-- recordid: string (nullable = true)
     |-- country: string (nullable = false)
     |-- name: string (nullable = true)
     |-- address1: string (nullable = true)
     |-- address2: string (nullable = true)
     |-- city: string (nullable = true)
     |-- state: string (nullable = true)
     |-- zip: string (nullable = true)
     |-- tel: string (nullable = true)
     |-- dateStart: string (nullable = true)
     |-- dateEnd: string (nullable = true)
     |-- type: string (nullable = true)
     |-- geoX: double (nullable = true)
     |-- geoY: double (nullable = true)
     |-- id: string (nullable = true)
     */

    /*
    *** Looking at partitions
    Partition count before repartition: 1
    Partition count after repartition: 4
     */

}




















































