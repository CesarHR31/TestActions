// Databricks notebook source
//System.setProperty("log4j2.configurationFile", "/dbfs/tmp/sat/diot/log4j2config.xml")


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

// DBTITLE 1,Import
import sat.diot.parser.{BronceCoord, OroCoord, PlataCoord}
import sat.diot.cifrascontrol.{CifrasControlCoord, TipoConteoCifras}
import sat.diot.conciliacion.{ConciliacionCoord, TipoConciliacion}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.comunes.config.ConfigurationProvider
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

// COMMAND ----------

class ConciliacionPlata
{
  lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
  val pgHandler = new PostgresqlHandler(ConfigurationProvider.postgresqlControlUrl)
  val db        = pgHandler.slickDatabase
  
  def generaConciliacion()
  { 
    new ConciliacionCoord().procesaPendientes(TipoConciliacion.plata)
    logger.info(Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf))
  }
  
  def generaCifrasDetalle()
  {
    new CifrasControlCoord().procesaPendientes(TipoConteoCifras.plata)
    logger.info(Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf))
  }
}

// COMMAND ----------

// DBTITLE 1,Main
val instancia = new ConciliacionPlata()
instancia.generaConciliacion()
instancia.generaCifrasDetalle()