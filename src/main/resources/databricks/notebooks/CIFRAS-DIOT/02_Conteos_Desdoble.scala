// Databricks notebook source
// DBTITLE 1,widgets
//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicial:")
dbutils.widgets.text("fechaFinal", "", "Fecha Final:")

// COMMAND ----------

import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes._
import java.time.Duration
import java.time.temporal.ChronoUnit
import  org.apache.spark.sql.functions._

/*Rango del delta*/
val fechaInicial = dbutils.widgets.get("fechaInicial")
val fechaFinal   = dbutils.widgets.get("fechaFinal")

/*Nombres de tablas de conteos de paso*/
val nombreTablaPasoConteosBronce = "default.ctl_diot_conciliacion_paso_bronce"
val nombreTablaPasoConteosCuarentena = "default.ctl_diot_conciliacion_paso_cuarentena"
val nombreTablaPasoConteosPlata = "default.ctl_diot_conciliacion_paso_plata"
val nombreTablaPasoConteosOro = "default.ctl_diot_conciliacion_paso_oro"
val nombreTablaPasoActualizar = "default.ctl_diot_conciliacion_paso_actualizar"
val nombreTablaPasoConteosLand = "default.ctl_diot_conciliacion_paso_land"

/*Nombres de tablas de conteos databricks*/
val nombreTablaConteosLand = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosLand).name
val nombreTablaConteosBronce = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosBronce).name
val nombreTablaConteosCuarentena = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosCuarentena).name
val nombreTablaConteosPlata = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosPlata).name
val nombreTablaConteosOro = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosOro).name

var InicioEjecucion = System.currentTimeMillis

// COMMAND ----------

// DBTITLE 1,Eliminamos tablas temporales de conteo
spark.sql(s"DROP TABLE IF EXISTS ${nombreTablaPasoConteosLand}")
spark.sql(s"DROP TABLE IF EXISTS ${nombreTablaPasoConteosBronce}")
spark.sql(s"DROP TABLE IF EXISTS ${nombreTablaPasoConteosCuarentena}")
spark.sql(s"DROP TABLE IF EXISTS ${nombreTablaPasoConteosPlata}")
spark.sql(s"DROP TABLE IF EXISTS ${nombreTablaPasoConteosOro}")



// COMMAND ----------

// DBTITLE 1,Funciones
def ObtieneMinutosEjecucion(InicioEjecucion:Long):Long =
{
  Duration.of(System.currentTimeMillis - InicioEjecucion, ChronoUnit.MILLIS).toMinutes.toLong
}

def OptimizaTabla(nombreTabla:String)
{
  spark.sql(s"""OPTIMIZE ${nombreTabla}""")
}

def ObtieneConteosTabla(nombreTablaOrigen:String, nombreTablaDestino:String, fechaInicial:String, fechaFinal:String, tieneParticion:Boolean = true)
{
  val campos = spark.sql(s"describe table ${nombreTablaOrigen}")
  val campoFecha = if(campos.filter(lower($"col_name") === "fechapresentacion").isEmpty) 
                   {
                    if(campos.filter(lower($"col_name") === "fechadeclaracion").isEmpty) 
                     "F_DEC_FPDEREC1"
                    else
                     "fechadeclaracion"                     
                   }
                   else "fechapresentacion"
  val campoParticion = "p_fechapresentacion"  

  val subquery = if(tieneParticion) {s"""(
                                          SELECT ${campoFecha}
                                          FROM ${nombreTablaOrigen}
                                          WHERE date(${campoParticion}) >= date_trunc('MM','$fechaInicial')
                                          AND date(${campoParticion}) <= date_trunc('MM','$fechaFinal')                                            
                                        )"""}
                 else{s"""${nombreTablaOrigen}"""}

    
  spark.sql(s"""SELECT date(${campoFecha}) fechaPresentacion, COUNT(1) contador, '${nombreTablaOrigen}' nombreTablaOrigen
                FROM ${subquery}
                WHERE date(${campoFecha}) >= date('$fechaInicial')
                  AND date(${campoFecha}) <= date('$fechaFinal')   
                GROUP BY date(${campoFecha})
                ORDER BY date(${campoFecha})""")
  .write.mode("append").format("delta").saveAsTable(nombreTablaDestino)     
}

def InsertaConteos(nombreTablaPasoConteos:String, nombreTablaConteos:String, TiempoEnMinutos:Long)
{  
  spark.sql(s"""SELECT A.nombreTablaOrigen nombreTabla, 
                       A.fechaPresentacion, 
                       A.contador, 
                       to_timestamp('${Util.obtenerTimestamp()}') fechaActualizacion, 
                       ${TiempoEnMinutos}L tiempoProcesamiento
                FROM ${nombreTablaPasoConteos} A LEFT JOIN ${nombreTablaConteos} B 
                     ON A.fechaPresentacion = B.fechaPresentacion AND A.nombreTablaOrigen = B.nombreTabla
                WHERE B.fechaPresentacion is null""")
  .write.mode("append").format("delta").saveAsTable(nombreTablaConteos) 
}

def ActualizaConteos(nombreTablaPasoConteos:String, nombreTablaConteos:String, TiempoEnMinutos:Long)
{  
  spark.sql(s"""MERGE INTO ${nombreTablaConteos} B
                USING ${nombreTablaPasoConteos} A
                ON A.fechaPresentacion = b.fechaPresentacion AND
                   A.nombreTablaOrigen = B.nombreTabla AND
                   A.contador != B.contador
                WHEN MATCHED THEN UPDATE 
                SET B.contador = A.contador, 
                    B.fechaActualizacion = to_timestamp('${Util.obtenerTimestamp()}'),
                    B.tiempoProcesamiento = ${TiempoEnMinutos}""")
}

// COMMAND ----------

// DBTITLE 1,Obtiene conteos Land
InicioEjecucion = System.currentTimeMillis
ObtieneConteosTabla(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name, nombreTablaPasoConteosLand, fechaInicial, fechaFinal)
InsertaConteos(nombreTablaPasoConteosLand, nombreTablaConteosLand, ObtieneMinutosEjecucion(InicioEjecucion))
ActualizaConteos(nombreTablaPasoConteosLand, nombreTablaConteosLand, ObtieneMinutosEjecucion(InicioEjecucion))
OptimizaTabla(nombreTablaConteosLand)

// COMMAND ----------

// DBTITLE 1,Obtiene conteos Bronce Validos
InicioEjecucion = System.currentTimeMillis
ObtieneConteosTabla(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name, nombreTablaPasoConteosBronce, fechaInicial, fechaFinal)
InsertaConteos(nombreTablaPasoConteosBronce, nombreTablaConteosBronce, ObtieneMinutosEjecucion(InicioEjecucion))
ActualizaConteos(nombreTablaPasoConteosBronce, nombreTablaConteosBronce, ObtieneMinutosEjecucion(InicioEjecucion))
OptimizaTabla(nombreTablaConteosBronce)

// COMMAND ----------

// DBTITLE 1,Obtiene conteos Cuarentena
InicioEjecucion = System.currentTimeMillis
ObtieneConteosTabla(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name, 
                    nombreTablaPasoConteosCuarentena, fechaInicial, fechaFinal)
InsertaConteos(nombreTablaPasoConteosCuarentena, nombreTablaConteosCuarentena, ObtieneMinutosEjecucion(InicioEjecucion))
ActualizaConteos(nombreTablaPasoConteosCuarentena, nombreTablaConteosCuarentena, ObtieneMinutosEjecucion(InicioEjecucion))
OptimizaTabla(nombreTablaConteosBronce)

// COMMAND ----------

// DBTITLE 1,Obtiene conteos Plata
InicioEjecucion = System.currentTimeMillis

EnumsTablas.listaTablasPlata.foreach(tabla => {  
  ObtieneConteosTabla(tabla.name, nombreTablaPasoConteosPlata, fechaInicial, fechaFinal)  
  println(s"Conteos Tabla: ${tabla.name}")
})

InsertaConteos(nombreTablaPasoConteosPlata, nombreTablaConteosPlata, ObtieneMinutosEjecucion(InicioEjecucion))  
ActualizaConteos(nombreTablaPasoConteosPlata, nombreTablaConteosPlata, ObtieneMinutosEjecucion(InicioEjecucion))  
OptimizaTabla(nombreTablaPasoConteosPlata)

// COMMAND ----------

// DBTITLE 1,Obtiene conteos Oro
InicioEjecucion = System.currentTimeMillis
EnumsTablas.listaTablasOro.foreach(tabla => {  
  println(s"Conteos Tabla: ${tabla.name}")
  ObtieneConteosTabla(tabla.name, nombreTablaPasoConteosOro, fechaInicial, fechaFinal)    
})

InsertaConteos(nombreTablaPasoConteosOro, nombreTablaConteosOro, ObtieneMinutosEjecucion(InicioEjecucion))  
ActualizaConteos(nombreTablaPasoConteosOro, nombreTablaConteosOro, ObtieneMinutosEjecucion(InicioEjecucion))  
OptimizaTabla(nombreTablaPasoConteosOro)

// COMMAND ----------

