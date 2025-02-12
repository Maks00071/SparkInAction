package net.jgp.books.spark.ch02.lab100_csv_to_db;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    /**
     * Метод выполняет следующие действия:
     * {1} Создание сеанса на локальном ведущем узле
     * {2} Считывание CSV-файла authors.csv с заголовком и сохраненние его содержимого в БД
     * {3} Создание нового столбца "name" как объединение столбца "lname", виртуального столбца,
     *     содержащего запятую с пробелом ", ", и столбца "fname"
     * {4} Методы concat() и lit() статично импортированны из org.apache.spark.sql.functions
     * {5} URL для установления соединения. Предполагается, что экземпляр PostgreSQL работает локально на
     *     стандартном порту (5432) и используется БД с именем "postgres"
     * {6} Свойста для установления соединения с БД. Драйвер JDBC прописывается в pom.xml
     * {7} Перезапись содержимого таблицы "spark_labs.ch02"
     */
    public void start() {

        SparkSession spark = SparkSession.builder()                             // {1}
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()                                          // {2}
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        df = df.withColumn(                                                     // {3}
                "name",
                concat(df.col("lname"), lit(", "),    // {4}
                                  df.col("fname")));

        String dbConnectionUrl = "jdbc:postgresql://localhost:5432/postgres";  // {5}
        Properties prop = new Properties();                                    // {6}
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "1987");

        df.write()                                                             // {7}
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "spark_labs.ch02", prop);

        System.out.println("Process complete!");
    }
}



























