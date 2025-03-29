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
                .format("net.jgp.books.spark.ch09.x.ds.exif." +
                        "ExifDirectoryDataSourceShortnameAdvertiser")                    // {2}
                .option("recursive", "true")              // {3}
                .option("limit", "100000")                // {4}
                .option("extensions", "jpg,jpeg")         // {5}
                .load(importDirectory);              // {6}

        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);

        return true;
    }

    /*
    I have imported 14 photos.
    root
     |-- MimeType: string (nullable = true)
     |-- Date: timestamp (nullable = true)
     |-- Directory: string (nullable = true)
     |-- Filename: string (nullable = false)
     |-- GeoX: float (nullable = true)
     |-- GeoZ: float (nullable = true)
     |-- Width: integer (nullable = true)
     |-- GeoY: float (nullable = true)
     |-- Height: integer (nullable = true)
     |-- FileCreationDate: timestamp (nullable = true)
     |-- FileLastModifiedDate: timestamp (nullable = true)
     |-- FileLastAccessDate: timestamp (nullable = true)
     |-- Name: string (nullable = true)
     |-- Size: long (nullable = true)
     |-- Extension: string (nullable = true)

    +----------+-------------------+--------------------+--------------------+---------+---------+-----+----------+------+--------------------+--------------------+--------------------+--------------------+-------+---------+
    |  MimeType|               Date|           Directory|            Filename|     GeoX|     GeoZ|Width|      GeoY|Height|    FileCreationDate|FileLastModifiedDate|  FileLastAccessDate|                Name|   Size|Extension|
    +----------+-------------------+--------------------+--------------------+---------+---------+-----+----------+------+--------------------+--------------------+--------------------+--------------------+-------+---------+
    |image/jpeg|2018-03-24 22:10:53|C:\Users\corys\Id...|C:\Users\corys\Id...|44.854095|254.95032| 3088| -93.24203|  2320|2025-03-10 21:10:...|2025-02-12 22:56:...|2025-03-29 14:04:...|A pal of mine (Mi...|1851384|      jpg|
    |image/jpeg|2018-03-31 17:47:01|C:\Users\corys\Id...|C:\Users\corys\Id...|     null|     null| 1620|      null|  1080|2025-03-10 21:10:...|2025-02-12 22:56:...|2025-03-29 14:04:...|Coca Cola memorab...| 589607|      jpg|
    |image/jpeg|2016-05-14 02:32:31|C:\Users\corys\Id...|C:\Users\corys\Id...|     null|     null| 5817|      null|  2317|2025-03-10 21:10:...|2025-02-12 22:56:...|2025-03-29 14:04:...|Ducks (Chapel Hil...|4218303|      jpg|
    |image/jpeg|2018-03-20 16:34:50|C:\Users\corys\Id...|C:\Users\corys\Id...|     null|     null| 1620|      null|  1080|2025-03-10 21:10:...|2025-02-12 22:56:...|2025-03-29 14:04:...|Ginni Rometty at ...| 469460|      jpg|
    |image/jpeg|2018-03-24 23:55:01|C:\Users\corys\Id...|C:\Users\corys\Id...|44.854633|    233.0| 1620|-93.239494|  1080|2025-03-10 21:10:...|2025-02-12 22:56:...|2025-03-29 14:04:...|Godfrey House (Mi...| 511871|      jpg|
    +----------+-------------------+--------------------+--------------------+---------+---------+-----+----------+------+--------------------+--------------------+--------------------+--------------------+-------+---------+
     */
}
