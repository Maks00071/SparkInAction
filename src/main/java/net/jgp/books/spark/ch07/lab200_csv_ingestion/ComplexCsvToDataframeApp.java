package net.jgp.books.spark.ch07.lab200_csv_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ComplexCsvToDataframeApp {

    public static void main(String[] args) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }

    /**
     * Основной метод
     * <p>
     * {1} - формат потребления данных (CSV)
     * {2} - наличие заголовка у файла
     * {3} - имеется вложенность в записях файла
     * {4} - разделитель значений в файле (";")
     * {5} - символом кавычек в файле является "*"
     * {6} - формат даты соответствует формату "месяц/день/год"
     * {7} - Spark логически выводит (самостоятельно определяет) схему
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")                   // {1}
                .option("header", "true")               // {2}
                .option("multiline", true)              // {3}
                .option("sep", ";")                     // {4}
                .option("quote", "*")                   // {5}
                .option("dateFormat", "M/d/y")          // {6}
                .option("inferSchema", true)            // {7}
                .load("data/ch07/books.csv");

        System.out.println("Except of the dataframe content: ");
        df.show(7, 90);

        System.out.println("Dataframe's schema: ");
        df.printSchema();
    }

    /*
    +---+--------+------------------------------------------------------------------------------------------+-----------+----------------------+
    | id|authorId|                                                                                     title|releaseDate|                  link|
    +---+--------+------------------------------------------------------------------------------------------+-----------+----------------------+
    |  1|       1|                          Fantastic Beasts and Where to Find Them: The Original Screenplay| 11/18/2016|http://amzn.to/2kup94P|
    |  2|       1|     Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Harry Potter; Book 1)| 10/06/2015|http://amzn.to/2l2lSwP|
    |  3|       1|                             The Tales of Beedle the Bard, Standard Edition (Harry Potter)| 12/04/2008|http://amzn.to/2kYezqr|
    |  4|       1|   Harry Potter and the Chamber of Secrets: The Illustrated Edition (Harry Potter; Book 2)| 10/04/2016|http://amzn.to/2kYhL5n|
    |  5|       2|Informix 12.10 on Mac 10.12 with a dash of Java 8: The Tale of the Apple; the Coffee; a...| 04/23/2017|http://amzn.to/2i3mthT|
    |  6|       2|Development Tools in 2006: any Room for a 4GL-style Language? \nAn independent study by...| 12/28/2016|http://amzn.to/2vBxOe1|
    |  7|       3|                                                            Adventures of Huckleberry Finn| 05/26/1994|http://amzn.to/2wOeOav|
    +---+--------+------------------------------------------------------------------------------------------+-----------+----------------------+
    only showing top 7 rows

    Dataframe's schema:
    root
     |-- id: integer (nullable = true)
     |-- authorId: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- releaseDate: string (nullable = true)
     |-- link: string (nullable = true)
     */

}

































