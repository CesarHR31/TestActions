// Databricks notebook source
// DBTITLE 1,Definicion entidades tablas de cifras
import java.util.Date
import java.sql.Timestamp
import spark.implicits._
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes._

val tablaListadoFechaPresentacion = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaListadoFechaPresentacion).name
val tablaListadoRecepcion = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaListadoRecepcion).name
val tablaContadorFP = ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaContadorFechaPresentacion).name
val tablaListadoUnicoReprocesos = ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaListadoUnicoReprocesos).name
val tablaFaltantesBronce = ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesLand).name

case class ListadoFechaPresentacion(
    idEjecucion: String,
    fecha: String
)

case class ListadoRecepcion(
    fechaPresentacion: String,
    rfc: String,
    numeroOperacion: String,
    timestamp: Timestamp,
    blobpath: String,
    obligaciones: String
)

case class ListadoUnicoReprocesos(
    fechaPresentacion: String,
    rfc: String,
    numeroOperacion: String,
    fechaActualizacion: Timestamp,
    blobpath: String,
    estatus: Int,
    archivoDestino: String,
    reprocesos: Int
)

// COMMAND ----------

// DBTITLE 1,Operaciones Tablas
import spark.implicits._
def registrarNuevoBatchListadoFechaPresentacion(listado: ListadoFechaPresentacion): Unit = {
    val dfListado = Seq(listado).toDF.withColumn("fecha", to_date($"fecha", "dd/MM/yyyy"))
    
  
val tablaOG = DeltaTable.forName(tablaListadoFechaPresentacion)


tablaOG
  .as("OG")
  .merge(
    dfListado.as("update"),
    "OG.fecha = update.fecha")
  .whenNotMatched
    .insertExpr(
    Map(
      "idEjecucion" -> "update.idEjecucion",
      "fecha" -> "update.fecha"
    ))
  .execute()

  }

def registrarListadoRecepcion(listado: DataFrame): Unit = {
  
  val tablaLR = DeltaTable.forName(tablaListadoRecepcion)  
  
tablaLR
  .as("LR")
  .merge(
    listado.as("update"),
    "LR.numeroOperacion = update.numeroOperacion")
  .whenNotMatched
  .insertExpr(
    Map(
      "numeroOperacion" -> "update.numeroOperacion",
      "FechaPresentacion" -> "update.FechaPresentacion",
      "rfc" -> "update.rfc",
      "Timestamp" -> "update.Timestamp",
      "Blobpath" -> "update.Blobpath",
      "p_fechapresentacion" -> "update.p_fechapresentacion"
    ))
  .execute()

  }

  def registrarListadoUnicoReprocesos(listadoUnico: DataFrame): Unit = {
  
  val DFRep = listadoUnico
  val tablaLR = DeltaTable.forName(tablaListadoUnicoReprocesos)  
  
tablaLR
  .as("LR")
  .merge(
    DFRep.as("update"),
    "LR.numeroOperacion = update.numeroOperacion")
  .whenNotMatched()
  .insertAll() 
  .execute()

  }

  def actualizarListadoUnicoReprocesos(estatus: Int, nombreArchivo: String): Unit = {
  
  val tablaLR = DeltaTable.forName(tablaListadoUnicoReprocesos)  
  
tablaLR.update(
  col("archivoDestino") === nombreArchivo,
  Map("estatus" -> lit(estatus),
      "fechaActualizacion" -> lit(Util.obtenerTimestamp())))
  
 }
    


// COMMAND ----------

def conteoTabla(nombreTabla: String, dfTabla: DataFrame): Unit ={
  
  val campoPart = if(nombreTabla.toLowerCase().equals(tablaListadoRecepcion.toLowerCase())) "p_fechapresentacion"
                  else "fechaPresentacion"
/*
  val campoPart = nombreTabla match {
    case tablaListadoRecepcion  => "p_fechapresentacion"
    case _ => "fechaPresentacion"
  }*/
  
val timestamp = new Timestamp(System.currentTimeMillis()).getTime();

  
  var tablaC = spark.table(nombreTabla).groupBy("p_fechapresentacion").count().as("contador")
                .withColumn("fechaActualizacion",current_timestamp())
                .join(dfTabla.withColumnRenamed("fecha", "p_fechapresentacion"), Seq("p_fechapresentacion"), "inner").drop("partitionkey")                
                .withColumnRenamed("count", "contador")
                .withColumnRenamed("p_fechapresentacion", "fechapresentacion")
  
  
val timestamp2 = new Timestamp(System.currentTimeMillis()).getTime();
 val diff = timestamp - timestamp2
  val diffMinutes = diff / (60 * 1000)
  
  tablaC = tablaC.withColumn("tiempoProcesamiento", lit(diffMinutes.toLong))
  
  
  val tablaLR = DeltaTable.forName(tablaContadorFP)  
  
  tablaLR.alias("tablaLR").merge(
    tablaC.alias("tablaC"),
    "tablaLR.idEjecucion = tablaC.idEjecucion") 
  .whenNotMatched()
  .insertAll()
  .whenMatched("tablaLR.contador <> tablaC.contador")
    .updateExpr(
    Map(
      "contador" -> "tablaC.contador",
      "fechaActualizacion" -> "tablaC.fechaActualizacion"
    ))  
  .execute()
  
  
}

// COMMAND ----------

