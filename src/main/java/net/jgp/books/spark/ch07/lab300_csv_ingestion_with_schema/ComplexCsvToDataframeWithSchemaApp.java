package net.jgp.books.spark.ch07.lab300_csv_ingestion_with_schema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.books.spark.ch07.utils.SchemaInspector;


public class ComplexCsvToDataframeWithSchemaApp {

    public static void main(String[] args) {
        ComplexCsvToDataframeWithSchemaApp app = new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    /**
     * The processing code
     * <p>
     * {1} - это один из способов создания схемы. В данном случае схема - это массив
     * значений типа StructField
     * {2} - имя поля - заменяет имена столбцов в исходном файле
     * {3} - определяем тип данных поля
     * {4} - допустимы ли в этом поле нулевые значения. Равнозначно: может ли поле принимать
     * значение null
     * {5} - сообщаем потоку считывания ( read() ) о необходимости использовать заданную схему
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{   // {1}
                DataTypes.createStructField(
                        "id",                                               // {2}
                        DataTypes.IntegerType,                                    // {3}
                        false),                                                   // {4}
                DataTypes.createStructField(
                        "authorId",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "bookTitle",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "releaseDate",
                        DataTypes.DateType,
                        true),
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false)});

        System.out.println("*************** schema ***************");
        SchemaInspector.print(schema);

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "MM/dd/yyyy")
                .schema(schema)                                                     // {5}
                .load("data/ch07/books.csv");

        System.out.println("\n*************** schema.json() ***************");
        SchemaInspector.print("Schema ...... ", schema);

        System.out.println("\n*************** df.schema() ***************");
        SchemaInspector.print("Dataframe ... ", df);

        System.out.println("\n***************************************************");

        df.show(5, 15);
        df.printSchema();
    }

    /*
    *************** schema ***************
    StructType(StructField(id,IntegerType,false),StructField(authorId,IntegerType,true),StructField(bookTitle,StringType,true),StructField(releaseDate,DateType,true),StructField(url,StringType,false))

    *************** schema.json() ***************
    Schema ...... {"type":"struct","fields":[
    *                                       {"name":"id","type":"integer","nullable":false,"metadata":{}},
    *                                       {"name":"authorId","type":"integer","nullable":true,"metadata":{}},
    *                                       {"name":"bookTitle","type":"string","nullable":true,"metadata":{}},
    *                                       {"name":"releaseDate","type":"date","nullable":true,"metadata":{}},
    *                                       {"name":"url","type":"string","nullable":false,"metadata":{}}
    *                                       ]}

    *************** df.schema() ***************
    Dataframe ... StructType(StructField(id,IntegerType,true),StructField(authorId,IntegerType,true),StructField(bookTitle,StringType,true),StructField(releaseDate,DateType,true),StructField(url,StringType,true))

    ***************************************************
    +---+--------+---------------+-----------+---------------+
    | id|authorId|      bookTitle|releaseDate|            url|
    +---+--------+---------------+-----------+---------------+
    |  1|       1|Fantastic Be...| 2016-11-18|http://amzn....|
    |  2|       1|Harry Potter...| 2015-10-06|http://amzn....|
    |  3|       1|The Tales of...| 2008-12-04|http://amzn....|
    |  4|       1|Harry Potter...| 2016-10-04|http://amzn....|
    |  5|       2|Informix 12....| 2017-04-23|http://amzn....|
    +---+--------+---------------+-----------+---------------+
    only showing top 5 rows

    root
     |-- id: integer (nullable = true)
     |-- authorId: integer (nullable = true)
     |-- bookTitle: string (nullable = true)
     |-- releaseDate: date (nullable = true)
     |-- url: string (nullable = true)
     */
}




































