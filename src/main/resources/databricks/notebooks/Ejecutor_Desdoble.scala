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

// DBTITLE 1,Import
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import sat.diot.comunes.CatalogoPasoEjecucionNPSI
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender
import sat.diot.comunes.Util
import sat.diot.comunes.config.ConfigurationProvider

// COMMAND ----------

// DBTITLE 1,Widgets
//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicial:")
dbutils.widgets.text("fechaFinal", "", "Fecha Final:")
dbutils.widgets.text("idEjecucion", "", "IdEjecucion:")
dbutils.widgets.text("horasHaciaAtras", "1", "Horas hacia atras:")
dbutils.widgets.text("rangoHoras", "1", "Rango de horas para ejecutar:")

// COMMAND ----------

// DBTITLE 1,Asignación de rango de ejecución
val t = ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDateTime
var idEjecucion = dbutils.widgets.get("idEjecucion").toString.toUpperCase

val horasHaciaAtras:Int = dbutils.widgets.get("horasHaciaAtras").toString.toInt
val rangoHoras:Int = dbutils.widgets.get("rangoHoras").toString.toInt
var fechaInicial = Timestamp.valueOf(t.minusHours(horasHaciaAtras).truncatedTo(ChronoUnit.HOURS))
var fechaFinal   = Timestamp.valueOf(t.minusHours(horasHaciaAtras - rangoHoras).truncatedTo(ChronoUnit.HOURS))

if(!dbutils.widgets.get("fechaInicial").trim().isEmpty && !dbutils.widgets.get("fechaFinal").trim().isEmpty)
{
  fechaInicial = Timestamp.valueOf(dbutils.widgets.get("fechaInicial"))
  fechaFinal   = Timestamp.valueOf(dbutils.widgets.get("fechaFinal"))
}

// COMMAND ----------

// DBTITLE 1,Land
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionLand
dbutils.notebook.run(
                     "1_Land", 
                     0,  
                     Map( 
                         "fechaInicial" -> fechaInicial.toString, 
                         "fechaFinal" -> fechaFinal.toString, 
                         "idEjecucion" -> idEjecucion                         
                       )
                   )
sat.diot.comunes.SparkSessionManager.sparkSession = spark
Util.declaracionVigenteLand(idEjecucion)

// COMMAND ----------

// DBTITLE 1,Bronce
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionBronce
dbutils.notebook.run("2_Bronce", 0)
sat.diot.comunes.SparkSessionManager.sparkSession = spark
Util.declaracionVigenteBronce(idEjecucion)

// COMMAND ----------

// DBTITLE 1,Plata
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionPlata
dbutils.notebook.run("3_Plata", 0 )
sat.diot.comunes.SparkSessionManager.sparkSession = spark
Util.declaracionVigentePlata(idEjecucion)

// COMMAND ----------

// DBTITLE 1,Conciliación Plata
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolPlata
dbutils.notebook.run("4_ConciliacionPlata", 0)

// COMMAND ----------

// DBTITLE 1,Oro
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionOro
dbutils.notebook.run("5_Oro", 0)
sat.diot.comunes.SparkSessionManager.sparkSession = spark
Util.declaracionVigenteOro(idEjecucion)

// COMMAND ----------

// DBTITLE 1,Conciliación Oro
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro
dbutils.notebook.run("6_ConciliacionOro", 0)

// COMMAND ----------

// DBTITLE 1,Generación CSV NPSI
ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.EnvioGZ
val pendientes = dbutils.notebook.run("7_CsvDeltas", 0)

// COMMAND ----------

// DBTITLE 1,Ultima declaración vigente a citus
dbutils.notebook.run("8_UltimaVigente", 0)

// COMMAND ----------

dbutils.notebook.exit(pendientes)