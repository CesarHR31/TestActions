package sat.diot.gzip.core

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import sat.diot.comunes.SparkSessionManager
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.ingesta.core.Excepciones.GZIPException

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable.ListBuffer

object GeneracionGZIP {

  lazy val spark: SparkSession = SparkSessionManager.session
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def generaGZIPDeTabla(entidadGold: String, directorioDestino: String = "", idEjecucion: String): Int = {

    var directorioDF = ""

    if (directorioDestino.isEmpty)
      directorioDF = "/tmp/sat/diot/tables/diotGZIP/"
    else
      directorioDF = directorioDestino

    try {

      val dfLand = spark
        .table(entidadGold)
        .drop("p_fechapresentacion")
        .drop("operacion")
        .drop("id")

      dfLand
        .coalesce(1)
        .where(col("idEjecucion") === idEjecucion)
        .withColumn("", lit(""))
        .write
        .format("com.databricks.spark.csv")
        .option("encoding", "utf-8")
        .option("header", "false")
        .option("nullValue", "\u0000")
        .option("emptyValue", "\u0000")
        .option("delimiter", "¬")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(directorioDF)

      var Archivo = ""
      for (file <- dbutils.fs.ls(directorioDF))
        if (file.name.endsWith(".gz"))
          Archivo = file.path
      dbutils.fs.cp(Archivo, directorioDF + ".txt.gz")
      dbutils.fs.rm(directorioDF, recurse = true)

      val nombreEntidadGold = entidadGold.split("\\.")(1)
      val nombreParaArchivoConteo = ConfigurationProvider
        .obtenerNombreDB2ParaTabla(nombreEntidadGold)
        .getOrElse(throw new Exception(s"Nombre no encontrado para $nombreEntidadGold"))

      dfLand
        .where(col("idEjecucion") === idEjecucion)
        .groupBy("numerooperacion", "rfcdeclarante", "ejercicio", "fechapresentacion")
        .agg(
          count("*").alias("conteo")
        )
        .join(
          spark.table("default.diot_numeros_operacion_tablas_oro"),
          Seq("numerooperacion", "rfcdeclarante", "ejercicio", "fechapresentacion"),
          "RIGHT"
        )
        .select(
          col("numerooperacion"),
          col("rfcdeclarante"),
          col("ejercicio"),
          lit("").alias("camponulo"),
          lit("diot").alias("tema"),
          col("fechapresentacion"),
          lit(nombreParaArchivoConteo).alias("nombrearchivo"),
          col("conteo")
        )
        .withColumn("conteo", when(col("conteo").isNotNull, col("conteo")).otherwise(0))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("default.diot_tablaTempConteosGzip")

      0
    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error durante la creacion del archivo GZIP", e)
        throw GZIPException(e.getMessage, e)
    }
  }

  def moverCvsHaciaAdls(rutaInterna: String, rutaExterna: String) {

    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("America/Mexico_City"))

    val c = Calendar.getInstance()
    c.setTimeZone(TimeZone.getTimeZone("America/Mexico_City"))

    val fileList = new ListBuffer[String]()
    for (f <- dbutils.fs.ls(rutaInterna)) {
      fileList += f.name.replace("/", "")
      println(f.name)
    }

    for (f <- fileList) {
      for (g <- dbutils.fs.ls(rutaInterna + s"/$f")) {
        if (g.name.startsWith("part-0000")) {
          println(g.name)
          dbutils.fs.mv(rutaInterna + s"/$f", rutaExterna + ".csv.gz", recurse = true)
        }
      }
    }
  }

  def replaceEmptyCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      when(col(c) === "", null).otherwise(col(c)).alias(c)
    })
  }

}
