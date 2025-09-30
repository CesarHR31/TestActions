package sat.diot.ingesta.core

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import sat.diot.comunes.Util.obtenerNumeroCoresEnCluster
import sat.diot.comunes._
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.EsquemasCapas.UltimaVigenteLand
import sat.diot.ingesta.utils.AzureTable

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZoneId}
import java.util.{Locale, TimeZone}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.immutable

object ultimaDeclaracionVigenteLand_backup {

  lazy val spark: SparkSession = SparkSessionManager.session
  lazy val jdbcUrl: String = ConfigurationProvider.postgresqlControlUrl
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  private val tiempoInicial = Util.obtenerTimestamp()
  private val tablaTemporalLandConcepto = "default.ultimavigenteland_diot_1"
  private val tablaTemporalLandConceptoDetalle = "default.ultimavigenteland_diot_2"
  private val DateFormat = "yyyy-MM-dd HH:mm:ss"

  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  def ultimaDeclaracionVigenteLandRun(
                                       fechaIni: Timestamp,
                                       fechaFin: Timestamp,
                                       idEjecucion: String
                                     ): Either[Throwable, Double] = {
    assert(fechaIni != null && fechaFin != null, "Fechas de procesamiento no pueden ser nulas.")

    val format = new SimpleDateFormat(s"$DateFormat", Locale.ROOT)
    val fi = format.format(fechaIni)
    val ff = format.format(fechaFin)

    logger.debug(s"Procesamiento de batch última vigente con ID $idEjecucion fechaIni $fechaIni fechafin $fechaFin")

    // Eliminamos tablas temporales
    try {
      if (spark.catalog.tableExists(tablaTemporalLandConcepto)) {
        spark.sql(s"DROP TABLE $tablaTemporalLandConcepto")
      }
    } catch {
      case e: Exception => null
    }
    try {
      if (spark.catalog.tableExists(tablaTemporalLandConceptoDetalle)) {
        spark.sql(s"DROP TABLE $tablaTemporalLandConceptoDetalle")
      }
    } catch {
      case e: Exception => null
    }

    logger.info("Comenzando proceso de ingesta WAT última vigente")

    val tiempoInicio = System.nanoTime
    logger.info(s"Cargando configuraciones de WAT última vigente")
    try {
      postgresqlHandler.obtenerConfiguracionWAT()
        .foreach { c =>
          logger.info(s"Procesando en cuenta última vigente: ${c.secret}")
          val resultadoIngesta = ingestaRun((fi, ff), idEjecucion, c.nombrewatconcepto, c.nombrewatconceptodetalle, dbutils.secrets.get(c.scope, c.secret))
          resultadoIngesta match {
            case Left(e) => throw new Exception(e.getStackTrace.mkString("Array(", ", ", ")"))
            case Right(value) =>
          }
        }

      spark
        .sql(s"""SELECT B.partitionkey, B.rowkey, A.timestamp, B.concepto, B.rfc, A.numerooperacion, B.fechadeclaracion,
                |A.ejercicio, A.periodicidad, A.periodo, A.tipodeclaracion, A.tipocomplementaria, B.estatusdeclaracion, A.identificadordeclaracion,
                |A.identificadordeclaracionpadre, A.identificadordeclaracionraiz, A.idejecucion, A.p_timestamp
                |FROM ${tablaTemporalLandConcepto} A
                |INNER JOIN  ${tablaTemporalLandConceptoDetalle} B
                |ON A.identificadordeclaracion = B.identificadordeclaracion""".stripMargin)
        .write
        .mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)

      logger.info(s"Terminado creacion de tabla última vigente Land")

      Right((System.nanoTime - tiempoInicio) / 1e9d)
    }
    catch {
      case e: Exception =>
        println(e.getStackTrace)
        Left(e)
    }
  }

  def ingestaRun(
                  rangoFechas: (String, String) = ("", ""),
                  idEjecucion: String,
                  watConsumoConcepto: String,
                  watConsumoConceptoDetalle:String,
                  secretScope: String
                ): Either[Throwable, Double] = {


    try {

      import spark.implicits._

      val particiones: Option[Int] = obtenerNumeroCoresEnCluster()
      val complementoConsulta = s""" and Ejercicio ge ${ConfigurationProvider.ejercicioDeclaracion.toString} and IdEstadoProceso eq ${ConfigurationProvider.idEstadoProceso.toString} and Timestamp ge datetime'${rangoFechas._1.replace(" ", "T")}' and Timestamp le datetime'${rangoFechas._2.replace(" ", "T")}'"""
      val listaQuerys = spark.table(ConfigurationProvider.identificadorTablaPartitionsKeysRfc).map( consulta => {
        consulta(0) + " " + complementoConsulta
      })


      val dfProcesar = particiones match {
        case Some(value) =>
          logger.debug(s"Reparticionando dataframe lista fechas en land a $value particiones.")
          listaQuerys
            .repartition(value)
        case None => listaQuerys.repartition(spark.table(ConfigurationProvider.identificadorTablaPartitionsKeysRfc).count().toInt)
      }

      val tablaLandCompleta = dfProcesar
        .mapPartitions { partition =>
          val storageAzure = new AzureTable(secretScope, watConsumoConcepto)
          partition.flatMap(row => {
            storageAzure
              .queryDeclaracionAnual(row)
              .asScala
              .toList
              .map(a => {
                val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
                val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
                val time = new SimpleDateFormat(s"$DateFormat")
                time.setTimeZone(TimeZone.getTimeZone("UTC"))
                val timeStamp = time.format(a.getTimestamp)
                UltimaVigenteLand(
                  a.getPartitionKey,
                  a.getRowKey,
                  Timestamp.valueOf(timeStamp),
                  a.getObligaciones,
                  null,
                  a.getRfc,
                  a.getNumeroOperacion,
                  Timestamp.valueOf(fechaDeclaracionFormato),
                  a.getEjercicio,
                  a.getPeriodicidad,
                  a.getPeriodo,
                  a.getTipoDeclaracion,
                  a.getTipoComplementaria,
                  a.getEstatusDeclaracion,
                  a.getIdentificadorDeclaracion.toString,
                  a.getIdentificadorDeclaracionPadre.toString,
                  a.getIdentificadorDeclaracionRaiz.toString,
                  idEjecucion
                )
              })
          })
        }
        .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))

      val obligaciones = ConfigurationProvider.obtenerObligaciones
      // Se agrega filtro por obligaciones permitidas
      tablaLandCompleta
        .filter($"obligaciones".contains(obligaciones))
        .write.mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(tablaTemporalLandConcepto)


      val complementoConsultaDetalle = s"""and Timestamp ge datetime'${rangoFechas._1.replace(" ", "T")}' and Timestamp le datetime'${rangoFechas._2.replace(" ", "T")}'"""
      val listaQuerysDetalle = spark.table(ConfigurationProvider.identificadorTablaPartitionsKeysRfcObligaciones).map(consulta => {
        consulta(0) + " " + complementoConsultaDetalle
      })


      val dfProcesarDetalle = particiones match {
        case Some(value) =>
          logger.debug(s"Reparticionando dataframe lista fechas en land a $value particiones.")
          listaQuerysDetalle
            .repartition(value)
        case None => listaQuerysDetalle.repartition(spark.table(ConfigurationProvider.identificadorTablaPartitionsKeysRfcObligaciones).count().toInt)
      }

      val tablaLandCompletaDetalle = dfProcesarDetalle
        .mapPartitions { partition =>
          val storageAzure = new AzureTable(secretScope, watConsumoConceptoDetalle)
          partition.flatMap(row => {
            logger.debug(s"consulta:${row}")
            storageAzure
              .queryDeclaracionAnual(row)
              .asScala
              .toList
              .map(a => {
                val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
                val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
                val time = new SimpleDateFormat(s"$DateFormat")
                time.setTimeZone(TimeZone.getTimeZone("UTC"))
                val timeStamp = time.format(a.getTimestamp)
                UltimaVigenteLand(
                  a.getPartitionKey,
                  a.getRowKey,
                  Timestamp.valueOf(timeStamp),
                  null,
                  a.getConcepto,
                  a.getPartitionKey.split("\\.")(0),
                  a.getNumeroOperacion,
                  Timestamp.valueOf(fechaDeclaracionFormato),
                  a.getEjercicio,
                  a.getPeriodicidad,
                  a.getPeriodo,
                  a.getTipoDeclaracion,
                  a.getTipoComplementaria,
                  a.getEstatus,
                  a.getIdentificadorDeclaracion.toString,
                  null,//a.getIdentificadorDeclaracionPadre.toString,
                  null,//a.getIdentificadorDeclaracionRaiz.toString,
                  idEjecucion
                )
              })
          })
        }
        .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))

      tablaLandCompletaDetalle
        .write.mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(tablaTemporalLandConceptoDetalle)



      Right(0L)
    } catch {
      case e: Exception => {
        logger.error(
          s"Ocurrio un error al tratar de procesar en ${e.getClass.getCanonicalName}, error : ${e.getMessage}, Stack : ${
            e.getStackTrace
              .mkString("Array(", ", ", ")")
          }"
        )
      }
        Left(e)
    }

  }


  def convierteFechasEnLista(fi: String, ff: String): immutable.Seq[(String, String)] = {

    val dateFormat = new SimpleDateFormat(s"$DateFormat")
    val zoneId = ZoneId.of("UTC") //Modificar zona horaria CDMX

    val fechaInicial = dateFormat.parse(fi).toInstant
    val fechaFinal = dateFormat.parse(ff).toInstant

    val duration = Duration.between(fechaInicial, fechaFinal)

    val dtFormatter = DateTimeFormatter
      .ofPattern(s"$DateFormat")
      .withZone(zoneId)

    (0L to duration.toDays - 1L).map { i =>
      val nuevaFechaInicial = fechaInicial.plus(i, ChronoUnit.DAYS)
      val nuevaFechaFinal = fechaInicial.plus(i + 1, ChronoUnit.DAYS)
      val result = (dtFormatter.format(nuevaFechaInicial), dtFormatter.format(nuevaFechaFinal))
      result
    }
  }


}
