package net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation;

import static org.apache.spark.sql.functions.concat;                             // {1}
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class IngestionSchemaManipulationApp {
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    /**
     * Метод выполняет следующий алгоритм:
     * {1}  - Импорт статических функций
     * {2}  - Создание сеанса Spark
     * {3}  - Создание фрейма данных (Dataset<Row>)
     * {4}  - Сохраняем заголовок файла CSV
     * {5}  - Загружаем CSV-файл
     * {6}  - Выводим 10 записей файла
     * {7}  - Выводим строение схемы файла
     * {8}  - Создаем новый столбец с именем "country", содержащий значение "Wake" в каждой записи
     * {9}  - Переименовываем столбцы в соответствии со схемой нового набора данных
     * {10} - Удаление указанных столбцов
     * {11} - Получаем доступ к RDD с помощью метода rdd(), затем переходим к разделам
     * {12} - Определяем кол-во существующих разделов (партиций)
     * {13} - Изменим организацию разделов для фрейма данных, чтобы использовтать 4 раздела
     * {14} - Получаем доступ к RDD с помощью метода rdd(), затем переходим к разделам
     * {15} - Извлечение схемы (type StruckType)
     * {16} - Вывод схемы в виде дерева
     * {17} - Вывод схемы в виде строки
     * {18} - Вывод схемы в формате JSON
     */
    private void start() {

       SparkSession spark = SparkSession.builder()                              // {2}
               .appName("Restaurants in Wake County, NC")
               .master("local")
               .getOrCreate();

       Dataset<Row> df = spark.read()                                           // {3}
               .format("csv")
               .option("header", "true")                                        // {4}
               .load("data/Lesson3/Restaurants_in_Wake_County_NC.csv");    // {5}

       System.out.println("*** Right after ingestion");
       df.show(5);                                                     // {6}

       df.printSchema();                                                        // {7}

        System.out.println("We have " + df.count() + " records.");

        df = df.withColumn("country", lit("Wake"))            // {8}
                .withColumnRenamed("HSISID", "datasetId")   // {9}
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")                                         // {10}
                .drop("PERMITID")
                .drop("GEOCODESTATUS");


        // Возможно, для каждой записи потребуется уникальный идентификатор.
        // Назовем данный столбец "id" и сформируем для него значения следующим образом:
        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),      // название штата + символ подчеркивания ("_")
                       df.col("country"), lit("_"),    // название округа + символ подчеркивания ("_")
                       df.col("datasetId")));                // идентификатор из исходного набора данных

        System.out.println("*** Dataframe transformed");
        df.show(5);
        df.printSchema();

        System.out.println("*******************************************************************");

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();                             // {11}
        int partitionCount = partitions.length;                                     // {12}
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);                                         // {13}
        System.out.println("Partition count after repartition: " + df.rdd().partitions().length);   // {14}

        System.out.println("******************************** Schema as a tree ***********************************");

        StructType schema = df.schema();                                           //{15}
        System.out.println("*** Schema as a tree: ");
        schema.printTreeString();                                                  //{16}

        System.out.println("***************************** Schema as string **************************************");

        String schemaAsString = schema.mkString();                                 //{17}
        System.out.println("*** Schema as string: " + schemaAsString);

        System.out.println("***************************** Schema as JSON **************************************");

        String schemaAsJson = schema.prettyJson();                                //{18}
        System.out.println("*** Schema as JSON: " + schemaAsJson);

    }
    // вывод работы метода
    /*  {6}
    *** Right after ingestion
    +--------+-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+--------+------------+-----------+-------------+
    |OBJECTID|     HSISID|                NAME|            ADDRESS1|ADDRESS2|       CITY|STATE|POSTALCODE|   PHONENUMBER|  RESTAURANTOPENDATE|     FACILITYTYPE|PERMITID|           X|          Y|GEOCODESTATUS|
    +--------+-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+--------+------------+-----------+-------------+
    |    1001|04092016024|                WABA|2502 1/2 HILLSBOR...|    NULL|    RALEIGH|   NC|     27607|(919) 833-1710|2011-10-18T00:00:...|       Restaurant|    6952|-78.66818477|35.78783803|            M|
    |    1002|04092021693|  WALMART DELI #2247|2010 KILDAIRE FAR...|    NULL|       CARY|   NC|     27518|(919) 852-6651|2011-11-08T00:00:...|       Food Stand|    6953|-78.78211173|35.73717591|            M|
    |    1003|04092017012|CAROLINA SUSHI &a...|5951-107 POYNER V...|    NULL|    RALEIGH|   NC|     27616|(919) 981-5835|2015-08-28T00:00:...|       Restaurant|    6961|-78.57030208|35.86511564|            M|
    |    1004|04092030288|THE CORNER VENEZU...|    7500 RAMBLE WAY |    NULL|    RALEIGH|   NC|     27616|          NULL|2015-09-04T00:00:...|Mobile Food Units|    6962|  -78.537511|35.87630712|            M|
    |    1005|04092015530|        SUBWAY #3726| 12233 CAPITAL BLVD |    NULL|WAKE FOREST|   NC|27587-6200|(919) 556-8266|2009-12-11T00:00:...|       Restaurant|    6972|-78.54097555|35.98087357|            M|
    +--------+-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+--------+------------+-----------+-------------+
     */

    /* {7}
    root
     |-- OBJECTID: string (nullable = true)             ---> drop()
     |-- HSISID: string (nullable = true)               ---> withColumnRenamed() ---> datasetId
     |-- NAME: string (nullable = true)                 ---> withColumnRenamed() ---> name
     |-- ADDRESS1: string (nullable = true)             ---> withColumnRenamed() ---> address1
     |-- ADDRESS2: string (nullable = true)             ---> withColumnRenamed() ---> address2
     |-- CITY: string (nullable = true)                 ---> withColumnRenamed() ---> city
     |-- STATE: string (nullable = true)                ---> withColumnRenamed() ---> state
     |-- POSTALCODE: string (nullable = true)           ---> withColumnRenamed() ---> zip
     |-- PHONENUMBER: string (nullable = true)          ---> withColumnRenamed() ---> tel
     |-- RESTAURANTOPENDATE: string (nullable = true)   ---> withColumnRenamed() ---> dateStart
     |-- FACILITYTYPE: string (nullable = true)         ---> withColumnRenamed() ---> type
     |-- PERMITID: string (nullable = true)             ---> drop()
     |-- X: string (nullable = true)                    ---> withColumnRenamed() ---> geoX
     |-- Y: string (nullable = true)                    ---> withColumnRenamed() ---> geoY
     |-- GEOCODESTATUS: string (nullable = true)        ---> drop()
                                                        ---> withColumn() ---> country
                                                        ---> withColumn() ---> id
     */

    // We have 3440 records.

    /*
    *** Dataframe transformed
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------------------+
    |  datasetId|                name|            address1|address2|       city|state|       zip|           tel|           dateStart|             type|        geoX|       geoY|country|                 id|
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------------------+
    |04092016024|                WABA|2502 1/2 HILLSBOR...|    NULL|    RALEIGH|   NC|     27607|(919) 833-1710|2011-10-18T00:00:...|       Restaurant|-78.66818477|35.78783803|   Wake|NC_Wake_04092016024|
    |04092021693|  WALMART DELI #2247|2010 KILDAIRE FAR...|    NULL|       CARY|   NC|     27518|(919) 852-6651|2011-11-08T00:00:...|       Food Stand|-78.78211173|35.73717591|   Wake|NC_Wake_04092021693|
    |04092017012|CAROLINA SUSHI &a...|5951-107 POYNER V...|    NULL|    RALEIGH|   NC|     27616|(919) 981-5835|2015-08-28T00:00:...|       Restaurant|-78.57030208|35.86511564|   Wake|NC_Wake_04092017012|
    |04092030288|THE CORNER VENEZU...|    7500 RAMBLE WAY |    NULL|    RALEIGH|   NC|     27616|          NULL|2015-09-04T00:00:...|Mobile Food Units|  -78.537511|35.87630712|   Wake|NC_Wake_04092030288|
    |04092015530|        SUBWAY #3726| 12233 CAPITAL BLVD |    NULL|WAKE FOREST|   NC|27587-6200|(919) 556-8266|2009-12-11T00:00:...|       Restaurant|-78.54097555|35.98087357|   Wake|NC_Wake_04092015530|
    +-----------+--------------------+--------------------+--------+-----------+-----+----------+--------------+--------------------+-----------------+------------+-----------+-------+-------------------+
     */

    /*
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
     |-- id: string (nullable = true)
     */

    /*
    *** Looking at partitions
    Partition count before repartition: 1
    Partition count after repartition: 4
     */

    /* {16}
    *** Schema as a tree:
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
     |-- id: string (nullable = true)
     */

    /*
    *** Schema as JSON: {
      "type" : "struct",
      "fields" : [ {
        "name" : "datasetId",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "name",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "address1",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "address2",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "city",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "state",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "zip",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "tel",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "dateStart",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "type",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "geoX",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "geoY",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "country",
        "type" : "string",
        "nullable" : false,
        "metadata" : { }
      }, {
        "name" : "id",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      } ]
    }
     */


}




























