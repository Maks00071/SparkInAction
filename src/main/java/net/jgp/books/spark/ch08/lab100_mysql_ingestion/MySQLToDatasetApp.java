package net.jgp.books.spark.ch08.lab100_mysql_ingestion;

import java.util.Properties;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class MySQLToDatasetApp {

    public static void main(String[] args) {
        MySQLToDatasetApp app = new MySQLToDatasetApp();
        app.start();
    }

    /**
     * {1} - создание объекта Properties, который будет использоваться для определения требуемых свойств <br>
     * {2} - свойство "user":"root" - имя пользователя БД (так делать не надо!) <br>
     * {3} - свойство "password":"1987" - пароль пользователя БД (так делать не надо!) <br>
     * {4} - особое свойство "useSSL":"false" - здесь мы сообщаем MySQL, что для установления
     * соединения не будет использоваться протокол SSL <br>
     * {5} - URL драйвера JDBC <br>
     * {6} -имя таблицы, в которой будет выполняться работа <br>
     * {7} - определение общего имени для набора перечисленных свойств <br>
     * {8} - сортировка по столбцу "last_name"
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local[*]")
                .getOrCreate();

        Properties props = new Properties();                                       // {1}
        props.put("user", "root");                                                 // {2}
        props.put("password", "1987");                                             // {3}
        props.put("useSSL", "false");                                              // {4}

        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/sakila?serverTimezone=EST", // {5}
                        "actor",                                                   // {6}
                        props);                                                    // {7}

        System.out.println("***************************** OUTPUT *****************************");
        System.out.println("***** Without sorting");
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");

        System.out.println("***** With sorting");
        df = df.orderBy(df.col("last_name"));                    // {8}
        df.show(5);
    }

    /*
    ***************************** OUTPUT *****************************
    ***** Without sorting
    +--------+----------+------------+-------------------+
    |actor_id|first_name|   last_name|        last_update|
    +--------+----------+------------+-------------------+
    |       1|  PENELOPE|     GUINESS|2006-02-15 12:34:33|
    |       2|      NICK|    WAHLBERG|2006-02-15 12:34:33|
    |       3|        ED|       CHASE|2006-02-15 12:34:33|
    |       4|  JENNIFER|       DAVIS|2006-02-15 12:34:33|
    |       5|    JOHNNY|LOLLOBRIGIDA|2006-02-15 12:34:33|
    +--------+----------+------------+-------------------+
    only showing top 5 rows

    root
     |-- actor_id: integer (nullable = true)
     |-- first_name: string (nullable = true)
     |-- last_name: string (nullable = true)
     |-- last_update: timestamp (nullable = true)

    The dataframe contains 200 record(s).
    ***** With sorting
    +--------+----------+---------+-------------------+
    |actor_id|first_name|last_name|        last_update|
    +--------+----------+---------+-------------------+
    |      92|   KIRSTEN|   AKROYD|2006-02-15 12:34:33|
    |      58| CHRISTIAN|   AKROYD|2006-02-15 12:34:33|
    |     182|    DEBBIE|   AKROYD|2006-02-15 12:34:33|
    |     118|      CUBA|    ALLEN|2006-02-15 12:34:33|
    |     145|       KIM|    ALLEN|2006-02-15 12:34:33|
    +--------+----------+---------+-------------------+
    only showing top 5 rows
     */
}































