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

// MAGIC %run ../DWH-NPSI/CopyHelper

// COMMAND ----------

import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.CatalogoPasoEjecucionNPSI
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender
import sat.diot.comunes._
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery
import java.time.Duration

val bufferSize = 32768
lazy val db = new PostgresqlHandler(ConfigurationProvider.postgresqlControlUrl)
val inicioEjecucion = System.currentTimeMillis

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
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
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

// DBTITLE 1,Truncamos Tabla en citus de vigentes
var connCitus: java.sql.Connection = null
try
{
  logueo.escribeLogueo(tipoLogueo.info,
                       "Se trunca tabla de última declaración vigente citus", 
                       CatalogoPasoEjecucionNPSI.EnvioCitus)
  connCitus = DriverManager.getConnection(SqlNpsiCitusContext.jdbcUrlCompleta, SqlNpsiCitusContext.connectionProperties)    
  var queryEliminarCitus = s"""TRUNCATE TABLE ${ConfigurationProvider.identificadorTablaUltimaDeclaracionVigente}""" 
  connCitus.prepareStatement(queryEliminarCitus).execute()
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.EnvioCitus)
    //throw new Exception(e)
}
finally
{
    connCitus.close()
}

// COMMAND ----------

// DBTITLE 1,Se genera lista de id ejecución para envio
val listaIdEjecucion = spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)
                       .selectExpr("idejecucion").distinct.map(_.getString(0))
                       .collect.toList

// COMMAND ----------

// DBTITLE 1,Enviamos data de oro databricks a citus
  
  listaIdEjecucion.foreach(id =>
  {
    try{
          logueo.escribeLogueo(tipoLogueo.info,
                               s"Se realiza el envio a citus de la información de última declaración vigente para el idEjecucion${id}",
                               CatalogoPasoEjecucionNPSI.EnvioCitus)
          val insumos = spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)
                             .filter($"idejecucion" === lit(id))
                             .selectExpr("rfc", "numerooperacion", "concepto", "fechadeclaracion", 
                                         "ejercicio", "periodicidad", "periodo", "tipodeclaracion", "tipocomplementaria", "estatusdeclaracion AS estatus",
                                         "identificadordeclaracion", "identificadordeclaracionpadre", "identificadordeclaracionraiz",
                                         "idejecucion AS idEjecucion")
          val contador = insumos.count
          CopyHelper.copyIn(SqlNpsiCitusContext.jdbcUrlCompleta,
                            insumos.coalesce(32),
                            ConfigurationProvider.identificadorTablaUltimaDeclaracionVigente, 
                            null, null, null, null, bufferSize, 
                            true) 
  
          db.registrarMonitoreoNPSI(id, 
                                    CatalogoPasoEjecucionNPSI.EnvioCitus,
                                    ConfigurationProvider.identificadorIdTablaUltimaDeclaracionVigenteCitus,
                                    ConfigurationProvider.identificadorTablaUltimaDeclaracionVigente,
                                    contador,
                                    contador,
                                    true,
                                    Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS)
                                     .toMillis.toInt,
                                     null
                                    )
          }
          catch{
            case e: Exception =>
              logueo.escribeLogueo(tipoLogueo.error, 
                                   e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                                   CatalogoPasoEjecucionNPSI.EnvioCitus)
               db.registrarMonitoreoNPSI(id, 
                                         CatalogoPasoEjecucionNPSI.EnvioCitus,
                                         ConfigurationProvider.identificadorIdTablaUltimaDeclaracionVigenteCitus,
                                         ConfigurationProvider.identificadorTablaUltimaDeclaracionVigente,
                                         0L,
                                         0L,
                                         false,
                                         Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS)
                                          .toMillis.toInt,
                                          e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")")
                                        )
    //throw new Exception(e)
        }                 
  })


// COMMAND ----------

