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

//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicial:")
dbutils.widgets.text("fechaFinal", "", "Fecha Final:")
dbutils.widgets.text("idEjecucion", "", "IdEjecucion:")

// COMMAND ----------

// DBTITLE 1,Import
import sat.diot.ingesta.core.IngestaCoord
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes._
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

// COMMAND ----------

// DBTITLE 1,Generar Hora para procesamiento batch
val idEjecucion = dbutils.widgets.get("idEjecucion").toString.toUpperCase
val fechaInicial = Timestamp.valueOf(dbutils.widgets.get("fechaInicial"))
val fechaFinal   = Timestamp.valueOf(dbutils.widgets.get("fechaFinal"))

// COMMAND ----------

// DBTITLE 1,Registrar batch
class Land{
  def registraBatch(fechaInicial:Timestamp, fechaFinal:Timestamp)
  {
    lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
    val pgHandler = new PostgresqlHandler(ConfigurationProvider.postgresqlControlUrl)
    val db        = pgHandler.slickDatabase    
    val infoBatch = new CifrasControlTable(
                                           idproc = idEjecucion,
                                           fi = fechaInicial,
                                           ff = fechaFinal,
                                           ConfigurationProvider.identificadorWATConsulta,
                                           false,
                                           ConfigurationProvider.configs.storageControlConnString.scope,                                           
                                           ConfigurationProvider.configs.storageControlConnString.key,                                          
                                           ConfigurationProvider.uriCuentaStorageArchivosJSON
    )
    pgHandler.registrarNuevoBatch(infoBatch)
    logger.info(Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf))
  }
}

// COMMAND ----------

new Land().registraBatch(fechaInicial, fechaFinal)

// COMMAND ----------

// DBTITLE 1,Procesar Land
IngestaCoord.procesaPendientes()

// COMMAND ----------

