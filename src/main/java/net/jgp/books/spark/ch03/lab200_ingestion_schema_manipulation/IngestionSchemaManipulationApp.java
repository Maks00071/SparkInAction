package net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation;

import static org.apache.spark.sql.functions.concat;                             // {1}
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class IngestionSchemaManipulationApp {
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    /**
     * Метод выполняет следующий алгоритм:
     * {1} - Импорт статических функций
     * {2} - Создание сеанса Spark
     * {3} - Создание фрейма данных (Dataset<Row>)
     * {4} - Сохраняем заголовок файла CSV
     * {5} - Загружаем CSV-файл
     * {6} - Выводим 10 записей файла
     * {7} - Выводим строение схемы файла
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
       df.show(5);                                                   // {6}

       df.printSchema();                                                      // {7}
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
     |-- OBJECTID: string (nullable = true)
     |-- HSISID: string (nullable = true)
     |-- NAME: string (nullable = true)
     |-- ADDRESS1: string (nullable = true)
     |-- ADDRESS2: string (nullable = true)
     |-- CITY: string (nullable = true)
     |-- STATE: string (nullable = true)
     |-- POSTALCODE: string (nullable = true)
     |-- PHONENUMBER: string (nullable = true)
     |-- RESTAURANTOPENDATE: string (nullable = true)
     |-- FACILITYTYPE: string (nullable = true)
     |-- PERMITID: string (nullable = true)
     |-- X: string (nullable = true)
     |-- Y: string (nullable = true)
     |-- GEOCODESTATUS: string (nullable = true)
     */
}




























