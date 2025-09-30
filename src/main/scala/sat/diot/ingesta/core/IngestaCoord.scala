package sat.diot.ingesta.core

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import sat.diot.comunes.Util.{generarRangoPartitionKeyWAT, obtenerNumeroCoresEnCluster}
import sat.diot.comunes._
import sat.diot.comunes.config.{ConfigurationProvider, EnumDefault}
import sat.diot.infraestructura.tablas._
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.{CatalogoProceso, RegistroMonitoreoNPSI}
import sat.diot.ingesta.entities.EsquemasCapas.Land
import sat.diot.ingesta.utils.AzureTable

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZoneId}
import java.util.{Locale, TimeZone}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.immutable

object IngestaCoord {

  lazy val spark: SparkSession = SparkSessionManager.session
  lazy val jdbcUrl: String = ConfigurationProvider.postgresqlControlUrl
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
  private val tiempoInicial = Util.obtenerTimestamp()
  private val StartSep = "Array("
  private val EndSep = ")"
  private val Separator = ", "

  def procesaPendientes(): Unit = {
    postgresqlHandler
      .obtenerBatchConEstatus(CatalogoProceso.REGISTRADO)
      .foreach { bi =>
        logger.info(s"Procesando ingesta batch ${bi.id_ejecucion}")
        ingestaRun(
          bi.fechaInicio,
          bi.fechaFin,
          bi.id_ejecucion,
          bi.esReproceso,
          bi.urlStorage
        )
      }
  }

  def ingestaRun(
                  fechaIni: Timestamp,
                  fechaFin: Timestamp,
                  idEjecucion: String,
                  esReproceso: Boolean,
                  cuentaUrl: String
                ): Either[Throwable, Double] = {
    assert(fechaIni != null && fechaFin != null, "Fechas de procesamiento no pueden ser nulas.")
    import spark.implicits._
    val format = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT)
    val fi = format.format(fechaIni)
    val ff = format.format(fechaFin)

    // Eliminamos tabla temporal
    val tablaTemporalLand = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_land_temp).name
    //try {
    if (spark.catalog.tableExists(tablaTemporalLand)) {
      spark.sql(s"DROP TABLE $tablaTemporalLand")
    }
    //    } catch {
    //      case e: Exception => null
    //    }

    val inicioEjecucion = System.currentTimeMillis
    logger.info("Comenzando proceso de ingesta")
    postgresqlHandler.insertaHistorico(idEjecucion)
    val tiempoInicio = System.nanoTime
    postgresqlHandler.actualizaIdEstatusBatch(idEjecucion, CatalogoProceso.PROCESANDOLAND)
    logger.info(s"Cargando configuraciones de WAT")
    try {
      postgresqlHandler.obtenerCuentasWAT()
        .foreach { c =>
          logger.info(s"Procesando en cuenta: ${c.secret}")
          val resultadoIngesta = ingestaRun((fi, ff), idEjecucion, c.wat, esReproceso, dbutils.secrets.get(c.scope, c.secret), cuentaUrl)
          resultadoIngesta match {
            case Left(e) => throw new Exception(e.getStackTrace.mkString(StartSep, Separator, EndSep))
            case Right(_) =>
          }
        }
      spark
        .table(tablaTemporalLand).distinct()
        .write
        .mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)

      logger.info("Tabla Land creada correctamente")

      val tablaLandConteos = spark
        .table(tablaTemporalLand)
        .where($"id_ejecucion" === lit(idEjecucion)).count()

      logger.info(s"Terminado creacion de tabla Land")

      postgresqlHandler.insertaBitacoraProcesoDatos(BitacoraProcesoDatos(
        idEjecucion,
        CONSTANTS.BITACORAS_ID_PROCESO,
        CONSTANTS.BITACORAS_PASO_RECEPCION,
        CatalogoEstatusProcesoDatos.Exito,
        None, tablaLandConteos,
        tiempoInicial,
        Util.obtenerTimestamp(),
        fechaIni,
        fechaFin, null, null
      ))

      postgresqlHandler.actualizaCifrasControlLand(
        CifrasControlTable(
          idEjecucion,
          null,
          null,
          tablaLandConteos,
          0L,
          0L,
          0L,
          0L,
          0L,
          Util.obtenerTimestamp(),
          CatalogoProceso.TERMINADOLANDEXITOSAMENTE,
          Util.obtenerTimestamp(),
          null,
          esReproceso,
          null,
          null,
          null
        )
      )

      logger.info(s"Terminada insercion de registros en cifras de control")

      postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        idEjecucion,
        CatalogoPasoEjecucionNPSI.IntegracionLand,
        ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).idTabla,
        ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name,
        tablaLandConteos,
        tablaLandConteos,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
      Right((System.nanoTime - tiempoInicio) / 1e9d)
    }
    catch {
      case e: Exception =>
        postgresqlHandler.actualizaIdEstatusBatch(idEjecucion, CatalogoProceso.ERRORENLAND)
        logger.error(
          s"Ocurrio un error al tratar de procesar en ${e.getClass.getCanonicalName}, error : ${e.getMessage}, Stack : ${
            e.getStackTrace
              .mkString(StartSep, Separator, EndSep)
          }"
        )
        Left(e)
    }
  }

  def ingestaRun(
                  rangoFechas: (String, String) = ("", ""),
                  idEjecucion: String,
                  watConsumo: String,
                  esReproceso: Boolean,
                  secretScope: String,
                  cuentaUrl: String
                ): Either[Throwable, Double] = {


    var rangoHorasProcesamiento = ("", "")

    if (rangoFechas._1.equals("") && rangoFechas._2.equals(""))
      rangoHorasProcesamiento = generarRangoPartitionKeyWAT()
    else {
      rangoHorasProcesamiento = rangoFechas
    }

    try {

      import spark.implicits._

      val particiones: Option[Int] = obtenerNumeroCoresEnCluster()

      val listaDeFechas = convierteFechasEnLista(rangoHorasProcesamiento._1, rangoHorasProcesamiento._2)

      logger.debug("Lista de fechas por procesar en land")

      val dfProcesar = particiones match {
        case Some(value) =>
          logger.debug(s"Reparticionando dataframe lista fechas en land a $value particiones.")
          listaDeFechas.toDF
            .repartition(value)
        case None => listaDeFechas.toDF
      }

      val tablaLandCompleta = dfProcesar
        .mapPartitions { partition =>
          val storageAzure = new AzureTable(secretScope, watConsumo)
          partition.flatMap { row =>
            storageAzure
              .queryParam(row.getString(0), row.getString(1), ConfigurationProvider.ejercicioDeclaracion)
              .asScala
              .toList
              .map(a => {
                val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
                val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
                Land(
                  a.getPartitionKey,
                  idEjecucion,
                  a.getRfc,
                  a.getNumeroOperacion,
                  a.getObligaciones,
                  Timestamp.valueOf(fechaDeclaracionFormato),
                  a.getEjercicio
                )
              })
          }
        }
        .withColumn("p_fechapresentacion", to_date(date_trunc("MM", col("FechaDeclaracion"))))
        .withColumn("blobpath", concat(lit(cuentaUrl), col("Rfc"), lit("."), col("NumeroOperacion"), lit(".json")))

      val tablaTemporalLand = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_land_temp).name
      val obligaciones = ConfigurationProvider.obtenerObligaciones
      // Se agrega filtro por obligaciones permitidas
      tablaLandCompleta.filter($"obligaciones".contains(obligaciones))
        .write
        .mode(SaveMode.Append)
        .format("delta")
        .saveAsTable(tablaTemporalLand)

      if (esReproceso) {
        logger.info(s"Se actualiza idEjecucion para el reproceso de la información")
        postgresqlHandler.actualizarEstatusReproceso(idEjecucion, 1, ConfigurationProvider.identificadorIdProceso)
      }

      Right(0L)
    } catch {
      case e: Exception => {
        logger.error(
          s"Ocurrio un error al tratar de procesar en ${e.getClass.getCanonicalName}, error : ${e.getMessage}, Stack : ${
            e.getStackTrace
              .mkString(StartSep, Separator, EndSep)
          }"
        )
        postgresqlHandler.actualizaEstadoBatch(
          CifrasControlTable(
            idEjecucion,
            null,
            null,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            null,
            CatalogoProceso.ERRORENLAND,
            Util.obtenerTimestamp(),
            null,
            esReproceso,
            null,
            null,
            null
          )
        )
      }
        postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          idEjecucion,
          CatalogoPasoEjecucionNPSI.IntegracionLand,
          ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).idTabla,
          ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name,
          0L,
          0L,
          exitoso = false,
          0,
          e.getStackTrace.mkString(StartSep, Separator, EndSep)
        ))
        Left(e)
    }

  }

  def convierteFechasEnLista(fi: String, ff: String): immutable.Seq[(String, String)] = {

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val zoneId = ZoneId.of("UTC")

    val fechaInicial = dateFormat.parse(fi).toInstant
    val fechaFinal = dateFormat.parse(ff).toInstant

    val duration = Duration.between(fechaInicial, fechaFinal)

    val dtFormatter = DateTimeFormatter
      .ofPattern(dateFormat.toPattern)
      .withZone(zoneId)

    (0L to duration.toHours - 1L).map { i =>
      val nuevaFechaInicial = fechaInicial.plus(i, ChronoUnit.HOURS)
      val nuevaFechaFinal = fechaInicial.plus(i + 1, ChronoUnit.HOURS)
      val result = (dtFormatter.format(nuevaFechaInicial), dtFormatter.format(nuevaFechaFinal))

      result
    }
  }

}
