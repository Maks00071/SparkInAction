package net.jgp.books.spark.ch09.lab400_photo_datasource;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class PhotoMetadataIngestionApp {

    public static void main(String[] args) {
        PhotoMetadataIngestionApp app = new PhotoMetadataIngestionApp();
        app.start();
    }

    /**
     * {1} - открываем ридер
     * {2} - определяем формат EXIF для ридера
     * {3} - необходима возможность рекурсивного чтения вложенных каталогов
     * {4} - устанавливается лимит в 100_000 файлов
     * {5} - источник данных считывает только файлы с расширениями JPG и JPEG
     * {6} - с какого каталога начинать импортирование данных
     */
    private boolean start() {

        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local[*]")
                .getOrCreate();

        String importDirectory = "data";

        Dataset<Row> df = spark.read()                    // {1}
                .format("exif")                    // {2}
                .option("recursive", "true")              // {3}
                .option("limit", "100000")                // {4}
                .option("extensions", "jpg,jpeg")         // {5}
                .load(importDirectory);              // {6}

        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);

        return true;
    }
}
