// Databricks notebook source
//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicial:")
dbutils.widgets.text("fechaFinal", "", "Fecha Final:")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.CatalogoPasoEjecucionNPSI
import sat.diot.comunes._
import sat.diot.infraestructura.applicationInsights.ApplicationInsightsAppender

/*Rango del delta*/
val fechaInicial = dbutils.widgets.get("fechaInicial").trim
val fechaFinal   = dbutils.widgets.get("fechaFinal").trim

// COMMAND ----------

case class camposOrden(campoFechaDeclaracion:String, campoIdEjecucion:String, campoNumeroOperacion:String)

def optimizar(tabla:String, particion:String, fechaInicio:String, fechaFinal:String, order:String)
{
  var zorder = if(order.length > 0){s"ZORDER BY (${order})"}
  println(s"""OPTIMIZE ${tabla} WHERE ${particion} < '${fechaFinal}' AND ${particion} >= '${fechaInicio}' ${zorder}""")
  spark.sql(s"""OPTIMIZE ${tabla} WHERE ${particion} < '${fechaFinal}' AND ${particion} >= '${fechaInicio}' ${zorder};""")
}
def devuelveCamposOrden(tabla:String):camposOrden =
{
  val campos = spark.sql(s"describe table ${tabla}")
                  .filter($"col_name" =!= "").filter(!$"col_name".contains("#"))
                  .select($"col_name")
                  .rdd.map(r => r(0).toString).collect().toList.mkString(",")

  var campoFechaDeclaracion = if(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name.toLowerCase().equals(tabla.toLowerCase())) "fechadeclaracion"  
                              else if(campos.toLowerCase().contains("f_dec_fpderec1")) "f_dec_fpderec1"                              
                              else "fechapresentacion"

  var campoIdEjecucion = if(campos.toLowerCase().contains("idejecucion")) "idejecucion"
                         else "idEjecucion"

  var campoNumeroOperacion = if(campos.toLowerCase().contains("numerooperacion")) "numerooperacion"
                             else "N_DEC_NOUPMEE1"  

  camposOrden(campoFechaDeclaracion, campoIdEjecucion, campoNumeroOperacion)
}

// COMMAND ----------

// MAGIC %sql 
// MAGIC set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false

// COMMAND ----------

// DBTITLE 1,Land
val campos = devuelveCamposOrden(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
val orden = campos.campoFechaDeclaracion + "," + campos.campoIdEjecucion + "," + campos.campoNumeroOperacion
optimizar(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name, "p_fechapresentacion", fechaInicial, fechaFinal, orden)

// COMMAND ----------

// DBTITLE 1,Bronce
val campos = devuelveCamposOrden(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
val orden = campos.campoFechaDeclaracion + "," + campos.campoIdEjecucion + "," + campos.campoNumeroOperacion
optimizar(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name, "p_fechapresentacion", fechaInicial, fechaFinal, orden)

// COMMAND ----------

// DBTITLE 1,Plata
EnumsTablas.listaTablasPlata.foreach(tabla => {  
  val campos = devuelveCamposOrden(tabla.name)
  val orden = campos.campoFechaDeclaracion + "," + campos.campoIdEjecucion + "," + campos.campoNumeroOperacion
  optimizar(tabla.name, "p_fechapresentacion", fechaInicial, fechaFinal, orden)
})

// COMMAND ----------

// DBTITLE 1,Oro
spark.sql("set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false")
EnumsTablas.listaTablasOro.foreach(tabla => {  
  val campos = devuelveCamposOrden(tabla.name)
  val orden = campos.campoFechaDeclaracion + "," + campos.campoIdEjecucion + "," + campos.campoNumeroOperacion
  optimizar(tabla.name, "p_fechapresentacion", fechaInicial, fechaFinal, orden)
})
spark.sql("set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = true")

// COMMAND ----------

