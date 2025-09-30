// Databricks notebook source
// DBTITLE 1,Widgets
//dbutils.widgets.removeAll()
dbutils.widgets.text("fechaInicial", "", "Fecha Inicio proceso:")
dbutils.widgets.text("fechaFinal", "", "Fecha Fin proceso:")

// COMMAND ----------

// DBTITLE 1,Utilerias
// MAGIC %run ./Utilerias/Comunes

// COMMAND ----------

// DBTITLE 1,Copy Helper
// MAGIC %run ./Utilerias/CopyHelper

// COMMAND ----------

// DBTITLE 1,Inicializa parámetro de entrada
import sat.diot.comunes.config.ConfigurationProvider
import org.apache.spark.sql.functions._
import sat.diot.comunes._

val regExFecha = """^(\d{4})-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$""".r

val paramIni = dbutils.widgets.get("fechaInicial").take(10)
val paramFin = dbutils.widgets.get("fechaFinal").take(10)

val fechaInicial = Date.valueOf(paramIni match {
  case regExFecha(x, y, z) => paramIni
  case "" => ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDate.minusDays(1).toString
  case _ => throw new IllegalArgumentException("Entrada no valida")
})

val fechaFinal = Date.valueOf(paramFin match {
  case regExFecha(x, y, z) => LocalDate.parse(paramFin).plusDays(1).toString
  case "" => ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDate.toString
  case _ => throw new IllegalArgumentException("Entrada no valida")
})

// COMMAND ----------

// DBTITLE 1,Obtener valores a procesar
//val tablasAgrupadas = ConfigurationProviderNB.obtenerTablasAContar

// Nombre que se le da a las columnas en el SP de PostgreSQL
// De no realizarse cambios estos valores podrian se estaticos
val llaves = Seq("idejecucion", "nombreTabla", "fechaPresentacion")

// COMMAND ----------


def devuelveCampoFechaPresentacion(tabla:String):String =
{
  val campos = spark.sql(s"describe table ${tabla}")
                  .filter($"col_name" =!= "").filter(!$"col_name".contains("#"))
                  .select($"col_name")
                  .rdd.map(r => r(0).toString).collect().toList.mkString(",").toLowerCase
  
  if(campos.contains("f_dec_fpderec1")) "f_dec_fpderec1"
  else "fechapresentacion"  
}

// COMMAND ----------

// DBTITLE 1,NPSI
val contadoresOro = spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosOro).name)
val listadoPresentacion = spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaListadoFechaPresentacion).name)

EnumsTablas.listaTablasOro.foreach(tabla => {
  val nombreTabla = tabla.name.replace(ConfigurationProvider.identificadorBaseOro.concat("."), "")
  val campo = devuelveCampoFechaPresentacion(tabla.name)
  println(s"Campo:${campo} nombreTabla:${nombreTabla}")
  var tablasContadores = Seq.empty[Contador].toDF()

  val (_ , tiempo) = Comunes.timeIt {
    try {
        tablasContadores = PSQL.obtenerContadoresDB(ConfigurationProvider.postgresqlCitusUrl, ConfigurationProvider.identificadorEsquemaCitus, nombreTabla, campo, fechaInicial, fechaFinal)
      } catch {
        case e: Exception =>
          println(s"Ocurrio un error, Tabla: ${nombreTabla} Error: $e")
          throw new Exception(e)
      }
    }

    val tiempoProcesamiento = Comunes.convertMilisecondToPart(tiempo, "minutos")

    if(!tablasContadores.isEmpty){
      val resultado = tablasContadores.as("A")
      // .join(listadoPresentacion, Seq("fechaPresentacion"))
      .join(listadoPresentacion, $"fechaPresentacion" === $"fecha")
      .withColumn("nombreTablaC", lit(nombreTabla))
      .join(contadoresOro.as("C"), $"A.fechaPresentacion" === $"C.fechaPresentacion" && $"nombreTablaC" === substring_index($"C.nombreTabla", ".", -1))
      .withColumnRenamed("contador", "contadororo")
      .withColumn("fechaActualizacion", current_timestamp)
      .withColumn("tiempoProcesamiento", lit(tiempoProcesamiento))
      .withColumn("estatus", when($"contadornpsi" === $"contadororo", lit(1)).otherwise(lit(2)))
      .select($"idejecucion", $"nombreTablaC".as("nombreTabla"), $"A.fechaPresentacion", $"contadororo", $"contadornpsi", $"estatus", $"fechaActualizacion", $"tiempoProcesamiento")

      Escritura.actualizaDelta(resultado, ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaContadorPostgreSql).name, llaves)
      // Escritura.escribirDelta(tablasContadores.withColumn("nombreTabla", lit(nombreTabla)), ConfigurationProviderNB.esquemaCTLDataBricks, "provisionalContadores", "append")
    } else {
      println(s"La consulta a la tabla ${nombreTabla} no generó registros")
    }
  
}
)
  
val tablaDestino = ConfigurationProvider.identificadorTablaContadorPSQLPostgreSQL
val tablaResultado = spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaContadorPostgreSql).name)
                     .withColumn("id_proceso", lit(ConfigurationProvider.identificadorIdProceso))



// COMMAND ----------

PSQL.truncaTabla(ConfigurationProvider.postgresqlControlUrl, tablaDestino)
CopyHelper.copyIn(ConfigurationProvider.postgresqlControlUrl, tablaResultado, tablaDestino)

// COMMAND ----------

// DBTITLE 1,Contadores reporte
val contadoresAplicativo = spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaContadorFechaPresentacion).name)
val contadoresDesdoble = spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosBronce).name)
                          .union(spark.table(ConfigurationProvider.obtenerTablaConciliacionId(EnumConciliacion.identificadorTablaConteosCuarentena).name))
                          .groupBy($"fechaPresentacion")
                          .agg(sum("contador").as("contador"), max("fechaActualizacion").as("fechaActualizacion"))

val resultado = contadoresAplicativo
  .withColumnRenamed("contador", "contadoraplicativo")
  .withColumnRenamed("fechaActualizacion", "fechaActualizacionAplicativo")
  .join(contadoresDesdoble, Seq("fechapresentacion"), "left")
  .withColumn("contadordesdoble", coalesce($"contador", lit(0)))
  .withColumn("fechaActualizacionDesdoble", coalesce($"fechaActualizacion", to_timestamp(lit("1753/01/01"), "yyyy/MM/dd")))
  .drop("contador", "fechaActualizacion")
  .withColumn("fechaActualizacion",
    when($"fechaActualizacionAplicativo" >= $"fechaActualizacionDesdoble", $"fechaActualizacionAplicativo")
    .otherwise($"fechaActualizacionDesdoble"))
  .withColumn("diferencia", abs($"contadoraplicativo" - $"contadordesdoble"))
  .withColumn("estado",
    when($"diferencia" > 0 && abs(datediff($"fechaActualizacionAplicativo", $"fechaActualizacionDesdoble")) >= 2, lit(3)).when($"diferencia" > 0, lit(2)).otherwise(lit(1)))
  .withColumn("fecha", date_format($"fechapresentacion", "yyyyMMdd"))
  .select($"fecha", $"fechapresentacion", $"contadoraplicativo", $"contadordesdoble", $"diferencia", $"fechaactualizacion", $"estado")

Escritura.escribirDelta(resultado, ConfigurationProvider.identificadorTablaContadorReporte.replace(ConfigurationProvider.identificadorEsquemaBitacorasSingle, ConfigurationProvider.identificadorBaseAF2Conciliacion), "overwrite")

val tablaDestino = ConfigurationProvider.identificadorTablaContadorReporte

PSQL.truncaTabla(ConfigurationProvider.postgresqlControlUrl, tablaDestino)
CopyHelper.copyIn(ConfigurationProvider.postgresqlControlUrl, 
                  resultado.withColumn("id_proceso", lit(ConfigurationProvider.identificadorIdProceso)), 
                  tablaDestino)

// COMMAND ----------

