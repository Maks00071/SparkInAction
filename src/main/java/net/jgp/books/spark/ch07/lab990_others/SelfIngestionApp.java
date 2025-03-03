package net.jgp.books.spark.ch07.lab990_others;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;  // Фабричный класс, используемый для создания объектов Row
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;


public class SelfIngestionApp {

    public static void main(String[] args) {
        SelfIngestionApp app = new SelfIngestionApp();
        app.start();
    }

    /**
     * Метод создает кастомную схему и Dataframe
     *
     * @param spark созданная Spark-сессия
     * @return Dataset<Row>
     */
    private static Dataset<Row> createDataframe(SparkSession spark) {

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "i",
                        DataTypes.IntegerType,
                        false)});

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Row> rows = new ArrayList<>();

        // создаем из каждого объекта "Integer" объект "Row" при помощи RowFactory.create(Object... values)
        for (int i : data) {
            rows.add(RowFactory.create(i));
        }
        return spark.createDataFrame(rows, schema);
    }

    /**
     *
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Self ingestion")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = createDataframe(spark);
        df.show(false);
        df.printSchema();

        // что "под капотом" у этой цепочки вызовов см. ниже
        long totalLines = df.selectExpr("sum(*)").first().getLong(0); // Dataframe -> [Dataset<Row>] -> [Row] -> long
        System.out.println(totalLines);

//        Dataset<Row> selExp = df.selectExpr("sum(*)");
//        System.out.println(selExp);  // [sum(i): bigint]
//        Row first =  selExp.first();
//        System.out.println(first);  // [90]
//        long number = first.getLong(0);
//        System.out.println(number); // 90



    }

    /*
    +---+
    |i  |
    +---+
    |1  |
    |2  |
    |3  |
    |4  |
    |5  |
    |6  |
    |7  |
    |8  |
    |9  |
    |1  |
    |2  |
    |3  |
    |4  |
    |5  |
    |6  |
    |7  |
    |8  |
    |9  |
    +---+

    root
     |-- i: integer (nullable = false)

    90
     */
}

















