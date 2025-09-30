// Databricks notebook source
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.LogManager
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender
import sat.diot.comunes.config.ConfigurationProvider

 
val context = LogManager.getContext(false).asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
val file = new java.io.File("/dbfs/tmp/sat/diot/log4j2config.xml");
 
// this will force a reconfiguration
context.setConfigLocation(file.toURI());

sat.diot.comunes.SparkSessionManager.sparkSession = spark

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import java.io.File
import java.nio.file.Files
import java.time._
import java.util.UUID
import java.util.Date
import java.util.UUID.randomUUID
import scala.util.Try
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZoneId}
import java.util.{Locale, TimeZone}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.immutable
import sat.diot.comunes.config.ConfigurationProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sat.diot.comunes.CatalogoPasoEjecucionNPSI
import sat.diot.comunes._
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender
import org.apache.hadoop.fs._

// COMMAND ----------

object tipoLogueo
{
  val info:Int = 1
  val debug:Int = 2
  val warn:Int = 3
  val error:Int = 4
}
object logueo extends Serializable
{
  lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
  def escribeLogueo(tipo:Int, mensaje:String, pasoEjecucion:Int){

    ApplicationInsightsAppender.pasoEjecucion = pasoEjecucion
    tipo match {
      case tipoLogueo.error => logger.error(mensaje)
      case tipoLogueo.warn => logger.warn(mensaje)
      case tipoLogueo.debug => logger.debug(mensaje)
      case _ => logger.info(mensaje)
    }   
  }
}

// COMMAND ----------

object  SqlNpsiCtlContext
{
  import java.util.Properties
  val jdbcHostname = dbutils.secrets.get("dwhPostgresKV","servidorPostgresControl")
  val jdbcPort = dbutils.secrets.get("dwhPostgresKV","puertoPostgres")
  val jdbcUsername = dbutils.secrets.get("dwhPostgresKV","usuarioPostgresControl")
  val jdbcPassword = dbutils.secrets.get("dwhPostgresKV","passPostgresControl")
  val jdbcDatabase = dbutils.secrets.get("dwhPostgresKV","baseDatosControl")
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}";
  val driverClass = "org.postgresql.Driver"
  Class.forName(driverClass)
  val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.put("password", jdbcPassword)
  connectionProperties.put("ssl","false")
  connectionProperties.put("sslmode","require")
  connectionProperties.setProperty("Driver", driverClass)
  val jdbcUrlCompleta = jdbcUrl + s"?user=${jdbcUsername}&password=${jdbcPassword}&sslmode=require"
  
  def aseguraConexionCtlNpsi(): Unit = {
    java.sql.DriverManager.getConnection(jdbcUrlCompleta, connectionProperties)
    println("PSQL Ctl OK!")
  }
}

// COMMAND ----------

SqlNpsiCtlContext.aseguraConexionCtlNpsi()

// COMMAND ----------

object  SqlNpsiCitusContext
{
  import java.util.Properties
  val jdbcHostname = dbutils.secrets.get("dwhPostgresKV","servidorPostgresDWH")
  val jdbcPort = dbutils.secrets.get("dwhPostgresKV","puertoPostgres")
  val jdbcUsername = dbutils.secrets.get("dwhPostgresKV","usuarioPostgresDWH")
  val jdbcPassword = dbutils.secrets.get("dwhPostgresKV","passPostgresDWH")
  val jdbcDatabase = dbutils.secrets.get("dwhPostgresKV","baseDatosDWH")
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}";
  val driverClass = "org.postgresql.Driver"
  Class.forName(driverClass)
  val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.put("password", jdbcPassword)
  connectionProperties.put("ssl","false")
  connectionProperties.put("sslmode","require")
  connectionProperties.setProperty("Driver", driverClass)
  val jdbcUrlCompleta = jdbcUrl + s"?user=${jdbcUsername}&password=${jdbcPassword}&sslmode=require"
  
  def aseguraConexionCitusNpsi(): Unit = {
    java.sql.DriverManager.getConnection(jdbcUrlCompleta, connectionProperties)
    println("PSQL Oro OK!")
  }
}

// COMMAND ----------

SqlNpsiCitusContext.aseguraConexionCitusNpsi()

// COMMAND ----------

val nombreTablaCsvFaltantesCuarentena = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaReingresoCsv).name
val tablaFaltantesCuarentena = "faltanesCuarentena"
val nombreTablaListadoFechaPresentacion = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaListadoFechaPresentacion).name

// COMMAND ----------

  def obtenerNumeroCoresEnCluster(): Option[Int] = {
    Try({
      val workers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt
      val cores: Int = Runtime.getRuntime.availableProcessors
      workers * cores
    }).toOption
  }

// COMMAND ----------

def obtenerLocalDateTime(zoneId: String = "America/Mexico_City"): LocalDateTime = 
{    
    java.time.ZonedDateTime.now(java.time.ZoneId.of(zoneId)).toLocalDateTime.plusHours(-1)// Se resta -1 por horario de verano
}
  
def obtenerTimestamp(zoneId: String = "America/Mexico_City"): java.sql.Timestamp = 
{
  val localDateTime = obtenerLocalDateTime(zoneId) 
  java.sql.Timestamp.valueOf(localDateTime)
}

// COMMAND ----------

  def convierteFechasEnLista(fi: String, ff: String): immutable.Seq[(String, String)] = {

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val zoneId = ZoneId.of("UTC")

    val fechaInicial = dateFormat.parse(fi).toInstant
    val fechaFinal = dateFormat.parse(ff).toInstant

    val duration = Duration.between(fechaInicial, fechaFinal)

    val dtFormatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(zoneId)

    (0L to duration.toHours - 1L).map { i =>
      val nuevaFechaInicial = fechaInicial.plus(i, ChronoUnit.HOURS)
      val nuevaFechaFinal = fechaInicial.plus(i + 1, ChronoUnit.HOURS)
      val result = (dtFormatter.format(nuevaFechaInicial), dtFormatter.format(nuevaFechaFinal))
      result
    }
  }

// COMMAND ----------

def obtieneNoEncontradosCuarentena(fechaInicial:String, fechaFinal:String)
{
  try
  {
    logueo.escribeLogueo(tipoLogueo.info, 
                         "Se obtienen no encontrados en cuarentena 404", 
                         CatalogoPasoEjecucionNPSI.GeneracionSolicitudFaltantes)
    val query = s"""SELECT * 
                    FROM (
                          SELECT numeroOperacion, rfc, fechaPresentacion, Id_Ejecucion, explode(errores) error
                          FROM (
                                SELECT *
                                FROM ${ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name}
                                WHERE p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                                 AND date_trunc('MM','$fechaFinal')
                               ) T1
                          WHERE T1.fechapresentacion >= '${fechaInicial}' AND T1.fechapresentacion <= '${fechaFinal}' AND
                                declaracionValida = false 
                         ) T2
                    WHERE T2. error.claveInformativa like '%ERROR_AL_DESCARGAR_BLOB%'
                  """
    spark.sql(query).createOrReplaceTempView(tablaFaltantesCuarentena)
}
catch{
    case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.GeneracionSolicitudFaltantes)
    throw new Exception(e)
   }
}

// COMMAND ----------

def borraFaltantesCuarentena(fechaInicial:String, fechaFinal:String, numerosOperacionABorrar:String)
{
  try
  {
    logueo.escribeLogueo(tipoLogueo.info, 
                         "Se borran no encontrados en cuarentena 404", 
                         CatalogoPasoEjecucionNPSI.EliminacionFaltantesCuarentena)
    val query = s"""DELETE FROM ${ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name} 
                    WHERE p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial') AND 
                          date_trunc('MM','$fechaFinal') AND 
                          numeroOperacion IN (${numerosOperacionABorrar})"""
    spark.sql(query)
  }
  catch{
    case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.EliminacionFaltantesCuarentena)
    throw new Exception(e)
   }
}

// COMMAND ----------

def GeneraArchivoCsvFaltantes(nombreArchivo:String, insumos:org.apache.spark.sql.DataFrame)
{
  try
  {
    logueo.escribeLogueo(tipoLogueo.info, 
                         "Se obtienen no encontrados en cuarentena 404", 
                         CatalogoPasoEjecucionNPSI.GeneracionSolicitudFaltantes)

    insumos
    .select($"numerooperacion", $"rfc")
    .repartition(1)
    .write
    .option("header", "false")
    .option("delimiter","|")
    .csv(ConfigurationProvider.identificadorPathCsvFaltantes + nombreArchivo.replace(".csv", ""))
  
    val file_path = dbutils.fs.ls(ConfigurationProvider.identificadorPathCsvFaltantes + nombreArchivo.replace(".csv", ""))
      .filter(file=>file.name.endsWith(".csv"))(0).path

    dbutils.fs.cp(file_path,ConfigurationProvider.identificadorPathCsvFaltantes + nombreArchivo)
    dbutils.fs.rm(ConfigurationProvider.identificadorPathCsvFaltantes + nombreArchivo.replace(".csv", ""), recurse = true)
   
  }
  catch{
    case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.GeneracionSolicitudFaltantes)
    throw new Exception(e)
   }
}

// COMMAND ----------

def GeneraCSVFaltantes(fechaInicial:String, fechaFinal:String, nombreArchivo:String)
{
  obtieneNoEncontradosCuarentena(fechaInicial, fechaFinal)
  if(spark.table(tablaFaltantesCuarentena).count > 0L)
  {    
    // Verifica que los faltantes no se encuentren en el listado de reproceso
    // o de existir que tengan permitido el reintento
    val query = s"""SELECT B.numeroOperacion numerooperacion, B.rfc
                FROM ${ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaListadoUnicoReprocesos).name} A 
                INNER JOIN ${tablaFaltantesCuarentena} B
                ON A.numeroOperacion = B.numeroOperacion
                WHERE A.reprocesos <= ${ConfigurationProvider.identificadorReintentosReprocesamiento}
                UNION
                SELECT B.numeroOperacion numerooperacion, B.rfc
                FROM ${ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaListadoUnicoReprocesos).name} A 
                RIGHT JOIN ${tablaFaltantesCuarentena} B
                ON A.numeroOperacion = B.numeroOperacion
                WHERE A.numeroOperacion IS NULL
                """
    val insumos = spark.sql(query).distinct

    GeneraArchivoCsvFaltantes(nombreArchivo, insumos)

    borraFaltantesCuarentena(fechaInicial, 
                             fechaFinal, 
                             insumos.select($"numerooperacion")
                                  .map(_.getLong(0))
                                  .collect.toList.mkString(","))    
  }
}

// COMMAND ----------

//GeneraCSVFaltantes("2023-04-08T12:50:03.014", "2023-06-11T12:50:03.014", "FC_20231017.csv")              
//obtieneNoEncontradosCuarentena("2023-03-31T13:38:39.429", "2023-07-23T13:38:39.429 ")              
//display(ejemplo)