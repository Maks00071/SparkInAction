package net.jgp.books.spark.ch07;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class NestedJson {

    public static void main(String[] args) {
        NestedJson app = new NestedJson();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Json to flatten table")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/Lesson3/nested.json");

        df.printSchema();
        df.show(5);

//        Dataset<Row> FDF = flattenDataframe(df);
//        FDF.printSchema();

        /*
        root
     |-- Topic: string (nullable = true)
     |-- Total_value: long (nullable = true)
     |-- values: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- points: array (nullable = true)
     |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |-- properties: struct (nullable = true)
     |    |    |    |-- date: string (nullable = true)
     |    |    |    |-- model: string (nullable = true)
     |    |    |-- value1: string (nullable = true)
     |    |    |-- value2: string (nullable = true)

     +-------+-----------+--------------------+
    |  Topic|Total_value|              values|
    +-------+-----------+--------------------+
    |Example|          3|[{[[123, 156]], {...|
    +-------+-----------+--------------------+
        */

    }

    /*
    private Dataset<Row> flattenDataframe(Dataset<Row> df) {

        List<String> fieldNames = new ArrayList<>();

        StructField[] fields = df.schema().fields();

        Arrays.stream(fields).map(StructField::name).forEach(fieldNames::add);
        System.out.println(fieldNames); // [Topic, Total_value, values]

        for (int i = 0; i < fieldNames.size(); i++) {
            StructField field = fields[i];
            DataType fieldType = field.dataType();
            String fieldName = field.name();
            System.out.println("field: " + field);
            System.out.println("fieldType: " + fieldType);
            System.out.println("fieldName: " + fieldName);

            if (fieldType instanceof ArrayType) {
                String firstFieldName = fieldName;
                List<String> fieldNamesExcludingArrayType = fieldNames.stream()
                        .filter(x -> x != firstFieldName)
                        .collect(Collectors.toList());
                List<String> explodeFieldNames = new ArrayList<>(fieldNamesExcludingArrayType);
                explodeFieldNames.addAll(Arrays.asList("explode_outer(" + firstFieldName + ") as " + firstFieldName));
                Dataset<Row> explodeDf = df.selectExpr(explodeFieldNames, );
                return flattenDataframe(explodeDf);

            } else if (fieldType instanceof StructType) {
                System.out.println("StructType");
            }
        }

        return df;
    }

    private String getString(List<String> list) {
        String result = "";

        for (int i = 0; i < list.size(); i++) {
            result += list.get(i);
        }
        return result;
    }
}
*/

/*
object json_to_scala_faltten {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("json-to-parquet").master("local[4]").getOrCreate()

    import spark.implicits._

    val flattenDF = spark.read.json(spark.createDataset(nestedJSON :: Nil))
    def flattenDF(df: DataFrame): DataFrame = {
      val fields = df.schema.fields
      val fieldNames = fields.map(x => x.name)
      for (i <- fields.indices) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          case aType: ArrayType =>
            val firstFieldName = fieldName
            val fieldNamesExcludingArrayType = fieldNames.filter(_ != firstFieldName)
            val explodeFieldNames = fieldNamesExcludingArrayType ++ Array(s"explode_outer($firstFieldName) as $firstFieldName")
            val explodedDf = df.selectExpr(explodeFieldNames: _*)
            return flattenDF(explodedDf)

          case sType: StructType =>
            val childFieldnames = sType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
            val explodedf = df.select(renamedcols: _*)
            return flattenDF(explodedf)
          case _ =>
        }
      }
      df
    }
val FDF = flattenDataframe(flattenDF)
FDF.show()
FDF.write.format("formatType").save("/path/filename")
  }
*/
}
















































