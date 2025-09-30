package sat.diot.comunes

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import sat.diot.parser.UtileriasParser

object SparkUtils {

  lazy val spark: SparkSession = SparkSessionManager.session

  import spark.implicits._

  def flattenSchema(
                     schema: StructType,
                     prefix: String = null
                   ): Array[(Column, String, Boolean, Option[String])] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case at: ArrayType =>
          at.elementType match {
            case a: StructType =>
              flattenSchema(at.elementType.asInstanceOf[StructType], colName + "[0]")
            case _ => Array((col(colName), f.dataType.toString, f.nullable, f.getComment()))
          }
        case _ => Array((col(colName), f.dataType.toString, f.nullable, f.getComment()))
      }
    })
  }

  def getSchemaInTabularFormat(tableName: String, databaseName: String): DataFrame = {

    val table = spark.table(s"$databaseName.$tableName")

    val fieldsFromTableSchema = flattenSchema(table.schema)
      .map { f => (f._1.toString, f._2, f._3, f._4) }
      .toSeq
      .toDF("Name", "DataType", "Nullable", "Metadata")

    val partitions = spark.sessionState.catalog
      .getTableMetadata(
        new org.apache.spark.sql.catalyst.TableIdentifier(tableName, Some(databaseName))
      )
      .partitionColumnNames

    val schema = new StructType()
      .add("metadata", StringType, nullable = true)
      .add("length", StringType, nullable = true)
      .add("claveSat", StringType, nullable = true)
      .add("tipoDato", StringType, nullable = true)
      .add("precisionDecimal", StringType, nullable = true)

    val NewFieldsFTS = fieldsFromTableSchema
      .withColumn("TipoDatoDatBrk", split($"DataType", "Type").getItem(0))
      .withColumn("LengthDatBrk", when(split($"DataType", "Type").getItem(1) === "", "n/a")
        .otherwise(split($"DataType", "Type").getItem(1)))
      .withColumn("IsPartition", $"Name".isin(partitions: _*))
      .withColumn("Database", lit(databaseName))
      .withColumn("Table", lit(tableName))
      .withColumn("JsonData", from_json($"metadata", schema))
      .withColumn("Descripcion", $"JsonData.metadata")
      .withColumn("ClaveSAT", $"JsonData.claveSat")
      .withColumn("TipoDatoPSQL", $"JsonData.tipoDato")
      .withColumn("LengthPSQL", when($"JsonData.length".isNull, $"JsonData.precisionDecimal")
        .otherwise($"JsonData.length"))
      .select(
        $"Name"
        , $"TipoDatoDatBrk"
        , $"Nullable"
        , $"Descripcion"
        , $"ClaveSAT"
        , $"IsPartition"
        , $"Database"
        , $"Table"
        , $"LengthDatBrk"
        , $"TipoDatoPSQL"
        , $"LengthPSQL"
        , $"Metadata"
      )

    NewFieldsFTS
  }

  def obtenerClavesSat(tableName: String, databaseName: String): Map[String, String] = {
    getSchemaInTabularFormat(tableName, databaseName)
      .select("Name", "ClaveSAT")
      .collect()
      .map { a =>
        val ruta = UtileriasParser.limpiarPath(a.getAs[String]("Name"))
        (ruta, a.getAs[String]("ClaveSAT"))
      }
      .map { k =>
        (k._1.replace("payload.", ""), k._2)
      }
      .toMap
  }
}
