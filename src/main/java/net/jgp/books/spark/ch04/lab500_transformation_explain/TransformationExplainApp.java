package net.jgp.books.spark.ch04.lab500_transformation_explain;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TransformationExplainApp {

    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        app.start();
    }

    /**
     * {1} - операция объединения "union"
     * {2} - набор операций: изменение столбцов - переименвание, математические операции и дублирование
     * {3} - исходные поля, взятые из CSV-файла
     * {4} - переименованные поля
     * {5} - столбец для среднего арифметического
     * {6} - дублируемые столбцы
     * {7} - cчитывание CSV-файла
     * {8} - поля, взятые из CSV-файла
     * {9} - формат файла
     * {10} - примечание: файл размещается в памяти
     * {11} - схема, выведенная Spark
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/ch04/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");

        Dataset<Row> df0 = df;

        df = df.union(df0);

        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        df = df.withColumn("avg", expr("(lcl + ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));

        // план выполнения для потребления ()
        df.explain();
    }

    /*
    операции располагаются в обратном порядке. Первая часть плана выполнения - объединение.
    == Physical Plan ==
    Union                                                   {1}
    :- *(1) Project                                         {2}
        [
            Year#17,                                        {3}
            State#18,                                       {3}
            County#19,                                      {3}
            State FIPS Code#20,                             {3}
            County FIPS Code#21,                            {3}
            Combined FIPS Code#22,                          {3}
            Birth Rate#23,                                  {3}
            Lower Confidence Limit#24 AS lcl#53,            {4}
            Upper Confidence Limit#25 AS ucl#63,            {4}
            ((cast(Lower Confidence Limit#24 as double) + cast(Upper Confidence Limit#25 as double)) / 2.0) AS avg#73,   {5}
            Lower Confidence Limit#24 AS lcl2#84,           {6}
            Upper Confidence Limit#25 AS ucl2#96            {6}
        ]
    :  +- FileScan csv                                      {7}
        [
            Year#17,                                        {8}
            State#18,                                       {8}
            County#19,                                      {8}
            State FIPS Code#20,                             {8}
            County FIPS Code#21,                            {8}
            Combined FIPS Code#22,                          {8}
            Birth Rate#23,                                  {8}
            Lower Confidence Limit#24,                      {8}
            Upper Confidence Limit#25                       {8}
        ] Batched: false,
          DataFilters: [],
          Format: CSV,                                      {9}
          Location: InMemoryFileIndex(1 paths)[file:/C:/Users/corys/IdeaProjects/SparkInAction/data/ch04/NCHS_-_Teen_...,   {10}
          PartitionFilters: [],
          PushedFilters: [],
          ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...  {11}
    +- *(2) Project
        [
            Year#35,
            State#36,
            County#37,
            State FIPS Code#38,
            County FIPS Code#39,
            Combined FIPS Code#40,
            Birth Rate#41,
            Lower Confidence Limit#42 AS lcl#109,
            Upper Confidence Limit#43 AS ucl#110,
            ((cast(Lower Confidence Limit#42 as double) + cast(Upper Confidence Limit#43 as double)) / 2.0) AS avg#111,
            Lower Confidence Limit#42 AS lcl2#112,
            Upper Confidence Limit#43 AS ucl2#113
        ]
       +- FileScan csv
        [
            Year#35,
            State#36,
            County#37,
            State FIPS Code#38,
            County FIPS Code#39,
            Combined FIPS Code#40,
            Birth Rate#41,
            Lower Confidence Limit#42,
            Upper Confidence Limit#43
        ] Batched: false,
          DataFilters: [],
          Format: CSV,
          Location: InMemoryFileIndex(1 paths)[file:/C:/Users/corys/IdeaProjects/SparkInAction/data/ch04/NCHS_-_Teen_...,
          PartitionFilters: [],
          PushedFilters: [],
          ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
     */
}





















