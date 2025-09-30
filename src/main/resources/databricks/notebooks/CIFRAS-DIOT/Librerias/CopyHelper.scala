// Databricks notebook source
import org.apache.spark.annotation.{Experimental, Unstable}
import org.apache.spark.sql.functions.{current_timestamp, from_utc_timestamp, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io.InputStream
import java.sql.DriverManager

// COMMAND ----------

object CopyHelper extends Serializable {
  
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  
 def rowsToInputStream(rows: Iterator[Row]): InputStream = {

    val bytes: Iterator[Byte] = rows
      .map { row =>
        (row.toSeq
          .map { v =>
            if (v == null /*|| v.toString.trim.isEmpty*/) {
              """\N"""
            } else {
              "\"" + v.toString.replaceAll("\u0000", "").replaceAll("\"", "\"\"") + "\""
            }
          }
          .mkString("\t") + "\n").getBytes("UTF-8")
      }
      .flatMap(a => a)

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) {
          bytes.next & 0xff // make the signed byte an unsigned int
        } else {
          -1
        }
    }
  }

  
def copyIn(url: String, df: DataFrame, table: String, EsPorNombreColumnas:Boolean):Long = {
    val bufferSize = 32768//65536
    val uuid = java.util.UUID.randomUUID()
    val longAccumulator = spark.sparkContext.longAccumulator(s"insercion-$uuid")
    var cols = df.columns.mkString(",")
    var ComandoCopiado = s"COPY $table " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')"""
    if(EsPorNombreColumnas)
      ComandoCopiado = s"COPY $table ($cols) " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')"""
    
    //println("bufferSize: " + bufferSize)
    df.rdd.foreachPartition { rows =>
      val conn = DriverManager.getConnection(url)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        val filasInsertadasCorrectamente = cm.copyIn(
          ComandoCopiado,
          rowsToInputStream(rows)
          ,bufferSize // se especifica buffer size
        )
        longAccumulator.add(filasInsertadasCorrectamente)
      } /*catch {
        case _: Throwable => System.err.println("Error while writing rows.")
      } */finally {
        conn.close()
      }
    }
    longAccumulator.value
  }  
}