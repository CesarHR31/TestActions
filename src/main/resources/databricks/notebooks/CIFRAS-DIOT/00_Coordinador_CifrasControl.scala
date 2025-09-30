// Databricks notebook source
//dbutils.widgets.removeAll()
dbutils.widgets.text("DiasSincronizacion", "5", "Días sincronizacion:") //Parámetro n
dbutils.widgets.text("DiaDelMes", "1", "Día de mes:") //Parámetro D
dbutils.widgets.text("DiasRetroceso", "90", "Historial en dias:") //Parametro N
dbutils.widgets.text("DiasHaciaAtras", "1", "Días hacia atras:") //Parámetro c

// COMMAND ----------

// MAGIC %run ./Librerias/Utils

// COMMAND ----------

import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.util.Date
import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.CatalogoPasoEjecucionNPSI
import sat.diot.comunes._
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender
import org.apache.spark.sql.functions._

val diasSincronizacion = dbutils.widgets.get("DiasSincronizacion").trim.toInt
val diaDelMes = dbutils.widgets.get("DiaDelMes").toString().toInt
val diasHaciaAtras = dbutils.widgets.get("DiasHaciaAtras").trim.toInt
val diasR   = dbutils.widgets.get("DiasRetroceso").toInt
val diaActual = ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDateTime.getDayOfMonth
val diasRetroceso = if(diaDelMes == diaActual)
                      diasR
                    else if((diaDelMes + 1) == diaActual)
                      diasR + 1
                    else
                      diasSincronizacion


val fechaFinal = ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDateTime.minusDays(1 + diasHaciaAtras)
val sdf = new SimpleDateFormat("dd/MM/yyyy")
val ejercicioFiltro   = ConfigurationProvider.ejercicioDeclaracion.toInt

val fechaInicial = fechaFinal.minusDays(diasRetroceso-1)
val formatter = new SimpleDateFormat("yyyyMMdd")
val nombreArchivo =  "FC_" + formatter.format(new Date()) + ".csv"

println(s"Fecha Inicio:$fechaInicial     Fecha Final: $fechaFinal \n")

// COMMAND ----------

// DBTITLE 1,Se obtiene listado recepción
try
{
  logueo.escribeLogueo(tipoLogueo.info, 
                       "Comienza listado Recepción", 
                       CatalogoPasoEjecucionNPSI.SincronizacionListadoRecepcion)

  dbutils.notebook.run("01_SincronizacionListadoRecepcion", 0, 
                       Map("FechaInicial" -> fechaInicial.toString, 
                           "FechaFinal" -> fechaFinal.toString, 
                           "EjercicioFiltro" -> ejercicioFiltro.toString))
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.SincronizacionListadoRecepcion)
    throw new Exception(e)
   }

// COMMAND ----------

// DBTITLE 1,Genera solicitud de faltantes cuarentena
GeneraCSVFaltantes(fechaInicial.toString, fechaFinal.toString, nombreArchivo)   

// COMMAND ----------

// DBTITLE 1,Se generan los conteos del desdoble en databricks
try
{
  logueo.escribeLogueo(tipoLogueo.info, 
                       "Se inicia con los conteos de desdoble DIOT",
                       CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
  dbutils.notebook.run("02_Conteos_Desdoble", 0, 
                       Map("fechaInicial" -> fechaInicial.toString.replace("T"," "), 
                           "fechaFinal" -> fechaFinal.toString.replace("T"," ")))
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
    throw new Exception(e)
   }

// COMMAND ----------

// DBTITLE 1,Se buscan los faltantes en databricks
try
{
  logueo.escribeLogueo(tipoLogueo.info, 
                       "Se buscan faltantes de desdoble DIOT",
                       CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
  dbutils.notebook.run("03_Buscar_Faltantes_Desdoble", 0, 
                       Map("fechaInicial" -> fechaInicial.toString.replace("T"," "), 
                           "fechaFinal" -> fechaFinal.toString.replace("T"," ")))
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
    throw new Exception(e)
   }

// COMMAND ----------

// DBTITLE 1,Se obtienen los conteos de tablas de citus
try
{
  logueo.escribeLogueo(tipoLogueo.info, 
                       "Se obtienen conteos NPSI DIOT",
                       CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
  dbutils.notebook.run("./DAPF_CifrasControl/ObtenerContadores", 0, 
                       Map("fechaInicial" -> fechaInicial.toString.replace("T"," "), 
                           "fechaFinal" -> fechaFinal.toString.replace("T"," ")))
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.BuscarFaltantesEnDesdoble)
    throw new Exception(e)
}

// COMMAND ----------

// DBTITLE 1,Se configuran reprocesos
try
{
  logueo.escribeLogueo(tipoLogueo.info, 
                       "Se inicia la configuración de reprocesos DIOY",
                       CatalogoPasoEjecucionNPSI.ConfiguracionReprocesos)
  dbutils.notebook.run("./Librerias/ConfiguraReprocesos", 0,
                     Map("nombreArchivo" -> nombreArchivo))
}
catch{
  case e: Exception =>
    logueo.escribeLogueo(tipoLogueo.error, 
                         e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")"), 
                         CatalogoPasoEjecucionNPSI.ConfiguracionReprocesos)
    throw new Exception(e)
}

// COMMAND ----------

try
{  
  dbutils.notebook.run("06_Optimize", 0, 
                       Map("fechaInicial" -> fechaInicial.toString.replace("T"," "), 
                           "fechaFinal" -> fechaFinal.toString.replace("T"," ")))
}
catch{
  case e: Exception =>    
    throw new Exception(e)
}