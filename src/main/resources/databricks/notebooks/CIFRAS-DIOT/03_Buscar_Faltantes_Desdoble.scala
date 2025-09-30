// Databricks notebook source
//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicial:")
dbutils.widgets.text("fechaFinal", "", "Fecha Final:")

// COMMAND ----------

import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes._
import java.time.Duration
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.functions._

// COMMAND ----------

/*Rango del delta*/
val fechaInicial = dbutils.widgets.get("fechaInicial")
val fechaFinal   = dbutils.widgets.get("fechaFinal")

/*Nombres de tablas de conteos databricks*/
val nombreTablaConteosLand = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosLand).name
val nombreTablaConteosBronce = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosBronce).name
val nombreTablaConteosCuarentena = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosCuarentena).name
val nombreTablaConteosPlata = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosPlata).name
val nombreTablaConteosOro = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosOro).name
val nombreTablaListadoRecepcion = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaListadoRecepcion).name

/*Nombres de tablas faltantes*/
val nombreTablaFaltantesLand =  ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesLand).name
val nombreTablaFaltantesBronce =  ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesBronce).name
val nombreTablaFaltantesPlata =  ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesPlata).name
val nombreTablaFaltantesOro =  ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesOro).name

var InicioEjecucion = System.currentTimeMillis

// COMMAND ----------

// DBTITLE 1,Funciones
def ObtieneConteosListado(nombreTabla:String, campoFiltro:String, fechaInicial:String, fechaFinal:String):Long =
{           
      spark.sql(s"""SELECT COUNT(1) 
                    FROM ${nombreTabla}
                    WHERE date(${campoFiltro}) >= date('$fechaInicial')
                        AND date(${campoFiltro}) <= date('$fechaFinal')   
                     """).head.getLong(0)
}
def ObtieneConteosPorTabla(nombreTabla:String, campoFiltro:String, fechaInicial:String, fechaFinal:String, tablaFiltro:String):Long =
{
      val filtroTabla = if(tablaFiltro.length == 0) "" else s" AND nombreTabla = '${tablaFiltro}'"      
      val conteo =spark.sql(s"""SELECT SUM(contador)
                                FROM ${nombreTabla}
                                WHERE date(${campoFiltro}) >= date('$fechaInicial')
                                 AND date(${campoFiltro}) <= date('$fechaFinal')                                 
                                       ${filtroTabla} GROUP BY nombreTabla """)
                                 
      if(conteo.isEmpty) 0L
      else conteo.head.getLong(0)
}

def ObtieneInsumos(nombreTabla:String, campoParticion:String, campoNumeroOperacion:String, 
                   campoFecha:String, fechaInicial:String, fechaFinal:String):org.apache.spark.sql.DataFrame =
{
      val condicion = if(campoParticion.length == 0) {""} 
                      else {s"""WHERE date(${campoParticion}) >= date_trunc('MM','$fechaInicial')
                                 AND date(${campoParticion}) <= date_trunc('MM','$fechaFinal')   """}
/*println(s"""SELECT date(${campoFecha}) fechaPresentacion, ${campoNumeroOperacion} numeroOperacion 
                    FROM ${nombreTabla} 
                    ${condicion}""")*/
      spark.sql(s"""SELECT date(${campoFecha}) fechaPresentacion, ${campoNumeroOperacion} numeroOperacion 
                    FROM ${nombreTabla} 
                    ${condicion}""")
}

def InsertaFaltantes(insumos:org.apache.spark.sql.DataFrame, nombreTablaFaltantes:String)
{
      insumos.alias("A").join(spark.table(nombreTablaFaltantes).alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left")
      .select($"A.numeroOperacion").createOrReplaceTempView("nuevobloque")
      
      // Borrar todo lo que no venga en el nuevo bloque
      spark.sql(s"""DELETE FROM ${nombreTablaFaltantes} WHERE numeroOperacion NOT IN (SELECT numeroOperacion FROM nuevobloque)""")
      
      insumos.alias("A").join(spark.table(nombreTablaFaltantes).alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "inner")
      .select($"A.numeroOperacion").createOrReplaceTempView("existentes")
      
      //Del nuevo bloque insertar solo lo que es nuevo
      insumos.alias("A").join(spark.table("existentes").alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left_anti")
      .select($"fechaPresentacion", $"numeroOperacion", lit(Util.obtenerTimestamp()).alias("fechaActualizacion"))
      .write.mode("append").format("delta").saveAsTable(nombreTablaFaltantes) 
}

// COMMAND ----------

// DBTITLE 1,Se obtienen Conteos
val conteoListadoRecepcion = 
    ObtieneConteosListado(nombreTablaListadoRecepcion, "p_fechaPresentacion", fechaInicial, fechaFinal)
val conteoLand = 
    ObtieneConteosPorTabla(nombreTablaConteosLand, "fechaPresentacion", fechaInicial, fechaFinal, 
                           ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
val conteoBronce = 
    ObtieneConteosPorTabla(nombreTablaConteosBronce, "fechaPresentacion", fechaInicial, fechaFinal, 
                           ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name)
val conteoCuarentena = 
    ObtieneConteosPorTabla(nombreTablaConteosCuarentena, "fechaPresentacion", fechaInicial, fechaFinal, 
                           ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name)
val conteoPlata = 
    ObtieneConteosPorTabla(nombreTablaConteosPlata, "fechaPresentacion", fechaInicial, fechaFinal, 
                           ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosidentideclaracion).name)
val conteoOro = 
    ObtieneConteosPorTabla(nombreTablaConteosOro, "fechaPresentacion", fechaInicial, fechaFinal, 
                           ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_datosidentideclaracion).name)

// COMMAND ----------

// DBTITLE 1,Listado Recepción vs Land
val insumosRecepcion = 
    ObtieneInsumos(nombreTablaListadoRecepcion, "p_fechaPresentacion", "numeroOperacion", 
                   "p_fechaPresentacion", fechaInicial, fechaFinal)
                  .withColumn("numeroOperacion",$"numeroOperacion".cast("BIGINT"))
val insumosLand = 
    ObtieneInsumos(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name, 
                   "p_fechaPresentacion", "numeroOperacion", 
                   "p_fechaPresentacion", fechaInicial, fechaFinal)                                    

val insumos = 
    insumosRecepcion.alias("A")
    .join(insumosLand.alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left_anti")
InsertaFaltantes(insumos, nombreTablaFaltantesLand)

// COMMAND ----------

// DBTITLE 1,Land vs Bronce
val insumosLand = 
   ObtieneInsumos(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name, 
                  "p_fechaPresentacion", "numeroOperacion", 
                  "p_fechaPresentacion", fechaInicial, fechaFinal)
val insumosBronce = 
   ObtieneInsumos(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name, 
                  "p_fechaPresentacion", "numeroOperacion", 
                  "p_fechaPresentacion", fechaInicial, fechaFinal)
 .union(ObtieneInsumos(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name, 
                       "p_fechaPresentacion", "numeroOperacion", "p_fechaPresentacion", fechaInicial, fechaFinal))

val insumos = insumosLand.alias("A").join(insumosBronce.alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left_anti")
InsertaFaltantes(insumos, nombreTablaFaltantesBronce)

// COMMAND ----------

// DBTITLE 1,Bronce vs Plata
val insumosBronce = 
   ObtieneInsumos(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name, 
                  "p_fechaPresentacion", "numeroOperacion", "p_fechaPresentacion", fechaInicial, fechaFinal)                                 
val insumosPlata = 
   ObtieneInsumos(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosidentideclaracion).name, 
                  "p_fechaPresentacion", "numeroOperacion", "p_fechaPresentacion", fechaInicial, fechaFinal)
val insumos = insumosBronce.alias("A").join(insumosPlata.alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left_anti")
InsertaFaltantes(insumos, nombreTablaFaltantesPlata)

// COMMAND ----------

// DBTITLE 1,Plata vs Oro
val insumosPlata = 
  ObtieneInsumos(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosidentideclaracion).name, 
                 "p_fechaPresentacion", "numeroOperacion", "p_fechaPresentacion", fechaInicial, fechaFinal)

val insumosOro = 
  ObtieneInsumos(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_datosidentideclaracion).name, 
                 "p_fechaPresentacion", "numeroOperacion", "p_fechaPresentacion", fechaInicial, fechaFinal)

val insumos = insumosPlata.alias("A").join(insumosOro.alias("B"), $"A.numeroOperacion" === $"B.numeroOperacion", "left_anti") 
InsertaFaltantes(insumos, nombreTablaFaltantesOro)