import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import scala.io.Source



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

}