package net.jgp.books.spark.ch09.other;

import java.io.*;
import java.util.Arrays;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ParseParquet {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ParseParquet app = new ParseParquet();
        app.start();
    }

    private void start() throws IOException, ClassNotFoundException {

        SparkSession spark = SparkSession.builder()
                .appName("Parquet")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("parquet")
                .load("data/parquet_exp/1685552974197_197000000_container_e71_1684334845828_2082_02_000002.parquet");

        df.printSchema();
        df.show(5);
        /*
        root
         |-- kafkaTimestampField: string (nullable = true)
         |-- kafka_partition_offset: string (nullable = true)
         |-- message: binary (nullable = true)

         +--------------------+----------------------+--------------------+
         | kafkaTimestampField|kafka_partition_offset|             message|
         +--------------------+----------------------+--------------------+
         |2023-05-30T16:07:...|             9_5016710|[4F 62 6A 01 02 1...|
         +--------------------+----------------------+--------------------+
         */


        byte[] dfBytes = df.select(df.col("message")).toString().getBytes();
        System.out.println(Arrays.toString(dfBytes));
        deserialize(dfBytes);

    }

    public static Object deserialize(byte [] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) return null;
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(b);
        return in.readObject();
    }
}


/*

// Example UDF to deserialize a Java object from a byte array
val deserializeUDF = udf { bytes: Array[Byte] =>
  val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
  val obj = ois.readObject()
  ois.close()
  obj
}

val deserializedDF = binaryFilesDF.withColumn("deserialized", deserializeUDF($"content"))
deserializedDF.show(truncate = false)

*/































