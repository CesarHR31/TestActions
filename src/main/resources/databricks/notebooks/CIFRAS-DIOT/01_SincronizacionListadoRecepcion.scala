// Databricks notebook source
dbutils.widgets.text("FechaInicial", "", "Fecha Inicio proceso:")
dbutils.widgets.text("FechaFinal", "", "Fecha Fin proceso:")
dbutils.widgets.text("EjercicioFiltro", "", "Filtro por ejercicio:")

// COMMAND ----------

// MAGIC %run ./Librerias/Utils

// COMMAND ----------

// MAGIC %run ./Librerias/DBDatabricks

// COMMAND ----------

// DBTITLE 1,Preparacion de intervalo de fechas
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.time.format.DateTimeFormatter
import java.util.Date
import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.ingesta.utils.AzureTable
import sat.diot.ingesta.entities.WATDescarga

val fechaInicial   = dbutils.widgets.get("FechaInicial")
val fechaFinal   = dbutils.widgets.get("FechaFinal")
val ejercicioFiltro   = dbutils.widgets.get("EjercicioFiltro")
val sdf = new SimpleDateFormat("dd/MM/yyyy")
val t = LocalDateTime.parse(fechaInicial)
val tf = LocalDateTime.parse(fechaFinal)

//val fechaInicial = t.minusDays(diasR)
val format = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT)
val fi = format.format(Timestamp.valueOf(t))
val ff = format.format(Timestamp.valueOf(tf))
val diasR = ChronoUnit.DAYS.between(t, tf.plusDays(1)).toInt

val listaDias = (0 to diasR -1).map{d => sdf.format(new Date(Timestamp.valueOf(t.plusDays(d).truncatedTo(ChronoUnit.HOURS)).getTime()))}
val particiones: Option[Int] = obtenerNumeroCoresEnCluster()
val listaDeFechas = convierteFechasEnLista(fi, ff)


val urlStorageJSON = ConfigurationProvider.uriCuentaStorageArchivosJSON

// COMMAND ----------

listaDias.foreach(dia => {
registrarNuevoBatchListadoFechaPresentacion(ListadoFechaPresentacion(java.util.UUID.randomUUID().toString.toUpperCase, dia))  
})

// COMMAND ----------

val consulta = spark.table(tablaListadoFechaPresentacion).filter($"fecha".geq(to_date(lit(listaDias(0).replace('/', '-')), "dd-MM-yyyy")))

// COMMAND ----------

val consultaPK = particiones match{
  
  case Some(value) =>
                             consulta.withColumn("PartitionKey", concat(year($"fecha"), 
                                                   format_string("%02d", month($"fecha")), 
                                                   format_string("%02d", dayofmonth($"fecha")),
                                                   )
                                                   )
                               .sort($"PartitionKey".asc)
                                .repartition(value)
    case None =>
                             consulta.withColumn("PartitionKey", concat(year($"fecha"), 
                                                   format_string("%02d", month($"fecha")), 
                                                   format_string("%02d", dayofmonth($"fecha")),
                                                   )
                                                   )
                               .sort($"PartitionKey".asc)
}

// COMMAND ----------

    val tablaTemporalLand = "default.am2_cifras_land_Homologacion"
    try {
      if (spark.catalog.tableExists(tablaTemporalLand)) {
        spark.sql(s"DROP TABLE $tablaTemporalLand")
      }
    } catch {
      case e: Exception => null
    }

// COMMAND ----------

import java.util.Date
def ingestaWAT(wat:String, sas:String)
{
  val tablaLandCompleta = 
  consultaPK
        .mapPartitions { partition =>
          val storageAzure = new AzureTable(sas, wat)
          partition.flatMap { row =>                  
            storageAzure
              .queryParamCifras(row.getString(2), ejercicioFiltro)
              .asScala
              .toList
              .map(a => {                
                ListadoRecepcion(
                  a.getFechaDeclaracion.toInstant.toString,
                  a.getRfc.toString(),
                  a.getNumeroOperacion.toString(),
                  Timestamp.from(a.getFechaDeclaracion.toInstant),                  
                  "",
                  a.getObligaciones       
                  )     
              })
              
          }
        }
        .withColumn("p_fechapresentacion", to_date(date_trunc("DD", col("fechaPresentacion"))))
        .withColumn("blobpath", concat(lit(urlStorageJSON), regexp_replace(to_date(col("fechaPresentacion")), "-", ""), lit("."), col("Rfc"), lit("."), col("NumeroOperacion"), lit(".json")))
      // Se agrega filtro por obligaciones permitidas
      tablaLandCompleta.filter($"obligaciones".contains("0313") ||$"obligaciones".contains("0314") || $"obligaciones".contains("0315") || $"obligaciones".contains("0316") || $"obligaciones".contains("0162") || $"obligaciones".contains("0163"))
        .drop("obligaciones")
        .write
        .mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(tablaTemporalLand)
}

// COMMAND ----------

lazy val jdbcUrl: String = ConfigurationProvider.postgresqlControlUrl
lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  postgresqlHandler.obtenerCuentasWAT()
  .foreach { c =>
    println(s"Procesando en cuenta: ${c.secret}")    
    ingestaWAT(c.wat:String, dbutils.secrets.get(c.scope, c.secret))  
  }        

// COMMAND ----------

registrarListadoRecepcion(spark.table(tablaTemporalLand))

// COMMAND ----------

conteoTabla(tablaListadoRecepcion, consultaPK)  