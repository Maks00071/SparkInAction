package net.jgp.books.spark.ch03.lab300_dataset;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;                                                   //{1}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ArrayToDatasetApp {

    public static void main(String[] args) {
        ArrayToDatasetApp app = new ArrayToDatasetApp();
        app.start();
    }

    /**
     * {1} - Encoders помогает создать набор данных для преобразования
     * {2} - создание статического массива с элементами
     * {3} - преобразование массива в список
     * {4} - создание набора данных из строк из списка и определение типа кодирования
     */
    private void start() {

       SparkSession spark = SparkSession.builder()
               .appName("Array to Dataset<String>")
               .master("local[*]")
               .getOrCreate();

       String[] stringList = new String[] {"Jean", "Liz", "Pierre", "Lauric"};           //{2}
       List<String> data = Arrays.asList(stringList);                                    //{3}
       Dataset<String> ds = spark.createDataset(data, Encoders.STRING());    //{4}
       ds.show();
       ds.printSchema();
    }

    /*
    +------+
    | value|
    +------+
    |  Jean|
    |   Liz|
    |Pierre|
    |Lauric|
    +------+

    root
     |-- value: string (nullable = true)
     */
}



























