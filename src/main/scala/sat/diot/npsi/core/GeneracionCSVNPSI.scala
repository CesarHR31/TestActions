package sat.diot.npsi.core

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import sat.diot.comunes.SparkSessionManager
import sat.diot.comunes.config.{ConfigurationProvider, SparkTable}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable.ListBuffer

object GeneracionCSVNPSI {
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
  lazy val spark: SparkSession = SparkSessionManager.session
  private val ZoneId = "America/Mexico_City"


  def getTimestamp(zoneId: String = ZoneId): java.sql.Timestamp = {
    val localDateTime = java.time.ZonedDateTime.now(java.time.ZoneId.of(zoneId)).toLocalDateTime
    java.sql.Timestamp.valueOf(localDateTime)
  }

  def obtenerConteosDeTablas(tablas: Seq[SparkTable], filtroTablaDestino: String, idEjecucion: String): Seq[ConteosDelta] = {
    tablas.map { tabla =>
      val c0 = spark.table(s"${tabla.name}").where(filtroTablaDestino).count

      ConteosDelta(idEjecucion, tabla.name.replace(s"${ConfigurationProvider.identificadorBaseOro}.", ""), c0)

    }.seq
  }

  def escribirDeltaCsv(df: DataFrame, rutaInterna: String, rutaExterna: String, nombreCSV: String): Unit = {

    df.withColumnRenamed("Entidad", "entidad")
      .select("idEjecucion", "entidad", "numeroRegistros")
      .coalesce(1)
      .withColumn("1", lit(""))
      .write
      .option("header", "false")
      .option("nullValue", "\u0000")
      .option("emptyValue", "\u0000")
      .option("delimiter", ",")
      .option("emptyValue", "\u0000")
      .option("nullValue", "\u0000")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      .option("charset", "UTF-8")
      .option("encoding", "UTF-8")
      .format("com.databricks.spark.csv")
      .save(rutaInterna + nombreCSV)

    moverCvsHaciaAdlsDelta(rutaInterna, rutaExterna)

    dbutils.fs.rm(rutaInterna + nombreCSV, true)

  }

  def moverCvsHaciaAdlsDelta(rutaInterna: String, rutaExterna: String) {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone(ZoneId))

    val c = Calendar.getInstance()
    c.setTimeZone(TimeZone.getTimeZone(ZoneId))

    val fileList = new ListBuffer[String]()
    for (f <- dbutils.fs.ls(rutaInterna)) {
      fileList += f.name.replace("/", "")
      println(f.name)
    }

    for (f <- fileList) {
      for (g <- dbutils.fs.ls(rutaInterna + f)) {
        if (g.name.startsWith("part-0000")) {
          println(g.name)
          dbutils.fs.mv(rutaInterna + s"/${f}/${g.name}", rutaExterna + s"${f}.csv", true)
        }
      }
    }
  }

  case class Conteos(
                      tabla: String,
                      conteoNuevo: Long,
                      conteoViejo: Long,
                      diferencia: Long,
                      filtrosOrigen: String,
                      filtrosDestino: String,
                      fechaConteo: java.sql.Timestamp
                    )

  case class ConteosDelta(idEjecucion: String, Entidad: String, numeroRegistros: Long)
}
