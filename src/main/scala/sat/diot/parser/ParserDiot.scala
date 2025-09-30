package sat.diot.parser

import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.specialized.BlobInputStream
import com.microsoft.azure.storage.blob.CloudBlockBlob
import org.apache.commons.lang3.StringUtils.left
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.jsfr.json.exception.JsonSurfingException
import sat.diot.comunes.Util.obtenerNumeroCoresEnCluster
import sat.diot.comunes._
import sat.diot.comunes.config.{ConfigurationProvider, EnumDefault}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.ingesta.entities.{CatalogoProceso, RegistroMonitoreoNPSI}

import java.net.URI
import java.sql.Timestamp
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.ForkJoinPool
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport

class ParserDiot extends Serializable {

  lazy val spark: SparkSession = SparkSessionManager.session
  lazy val jdbcUrl: String = ConfigurationProvider.postgresqlControlUrl
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  private val tiempoInicial = Util.obtenerTimestamp()
  private val ExpPartitionColumn = "to_date(date_trunc('MM', fechaPresentacion))"
  private val tablaTemporalBronce = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_bronce_temp).name
  private val tablaSizeTemp = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_size).name

  import spark.implicits._

  def procesar(dataFrame: Dataset[MetadataIngesta], fechaInicio: Timestamp, fechaFin: Timestamp): Unit = {
    val inicioEjecucion = System.currentTimeMillis
    val conf = ConfigurationProvider
    val jdbcUrl = conf.postgresqlControlUrl
    val tablaBronce = ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name

    val data = SparkUtils
      .obtenerClavesSat(tablaBronce.split("\\.")(1), conf.identificadorBaseBronce)

    assert(!dataFrame.isEmpty, "El dataframe esta vacío.")

    assert(
      dataFrame.select($"id_ejecucion").distinct().count() == 1,
      "El dataframe cuenta con mas de un identificador de batch."
    )

    val idBatch = dataFrame.select($"id_ejecucion").head().getString(0)

    getJsonBlobSize(dataFrame)

    val insumosSize = spark.table(tablaSizeTemp).as[MetadataValidaSize]

    if (spark.catalog.tableExists(tablaTemporalBronce))
      spark.sql(s"DROP TABLE $tablaTemporalBronce")

    //FLUJO NORMAL
    if (insumosSize.filter($"tipoFlujo" <= 1).count > 0) {
      ejecutaFlujoNormal(insumosSize, data)
    }
    //FLUJO PARA ARCHIVOS DE GRAN VOLUMEN
    if (insumosSize.filter($"tipoFlujo" === 2).count > 0) {
      ejecutaFlujoArchivosGranVolumen(insumosSize, data)
    }
    //ARCHIVOS MUY GRANDES, QUE EXECEDEN EL LíMITE SOPORTADO
    if (insumosSize.filter($"tipoFlujo" === 3).count() > 0) {
      val jsonToCaseClasses = new JsonToCaseClasses(data)
      insumosSize.filter($"tipoFlujo" === 3).map { eLargeFile =>
        jsonToCaseClasses.obtenerErrores.add(
          ErrorParser(
            "LÍMITE DE DOCUMENTO EXCEDIDO",
            null,
            null,
            s"El documento " +
              s"${eLargeFile.numerooperacion} excede el límite soportado."
          )
        )
        Bronce(
          eLargeFile.numerooperacion.toLong,
          eLargeFile.rfc,
          eLargeFile.fechadeclaracion,
          eLargeFile.id_ejecucion,
          eLargeFile.blobpath,
          declaracionValida = jsonToCaseClasses.obtenerErrores.isEmpty,
          Util.obtenerTimestamp(),
          jsonToCaseClasses.obtenerErrores.asScala.toArray,
          None
        )
      }
    }

    spark
      .table(tablaTemporalBronce)
      .write
      .format("delta")
      .partitionBy("p_fechapresentacion")
      .mode(SaveMode.Append)
      .saveAsTable(tablaBronce)

    val th = spark
      .table(tablaTemporalBronce)

    val contadorBronceValidos = th.where($"declaracionValida").count
    val contadorCuarentena = th.where(!$"declaracionValida").count

    val pgHandler = new PostgresqlHandler(jdbcUrl)

    pgHandler.actualizaCifrasControlBronce(
      idBatch,
      th.count,
      contadorBronceValidos,
      contadorCuarentena
    )

    pgHandler.actualizarEstatusBatch(idBatch, CatalogoProceso.TERMINADOBRONCEEXITOSAMENTE)

    // Se manda Bronce correctos
    pgHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
      idBatch,
      CatalogoPasoEjecucionNPSI.IntegracionBronce,
      ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).idTabla,
      ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name,
      contadorBronceValidos,
      contadorBronceValidos,
      exitoso = true,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
      null
    ))

    // Se manda Bronce Cuarentena
    pgHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
      idBatch,
      CatalogoPasoEjecucionNPSI.IntegracionBronce,
      ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).idTabla,
      ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name,
      contadorCuarentena,
      contadorCuarentena,
      exitoso = true,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
      null,
      esCuarentena = true
    ))

    pgHandler.insertaBitacoraProcesoDatos(BitacoraProcesoDatos(
      idBatch,
      CONSTANTS.BITACORAS_ID_PROCESO,
      CONSTANTS.BITACORAS_PASO_DESDOBLE,
      CatalogoEstatusProcesoDatos.Exito,
      None, contadorBronceValidos + contadorCuarentena,
      tiempoInicial,
      Util.obtenerTimestamp(),
      fechaInicio,
      fechaFin, null, null
    ))

    //RN-01 Si el contador de recepción es diferente al contador de bronce + cuarentenea,
    // el registro de desdoble se guarda con estatus de Alerta, en la descripción colocar: “El número de registros procesados es menor al número de registros recibidos”
    if (dataFrame.count() != contadorBronceValidos + contadorCuarentena) {
      pgHandler.actualizaEstatusProcesoDatos(
        idBatch,
        CONSTANTS.BITACORAS_PASO_DESDOBLE,
        CONSTANTS.BITACORAS_ID_PROCESO,
        CatalogoEstatusProcesoDatos.Alerta,
        "El número de registros procesados es menor al número de registros recibidos"
      )
    }
    //RN-02 Si el porcentaje de cuarentena es mayor al 2%(valor de configuración)
    // el registro de desdoble se guarda con estatus de alerta, en la descripción colocar el texto: “El porcentaje de cuarentena supera el X% de la recepción exitosa”
    val contadorRegistrosPorcentaje = ((contadorBronceValidos + contadorCuarentena) * ConfigurationProvider.identificadorPorcentajeCuarentenaPermitido) / 100
    if (contadorCuarentena > contadorRegistrosPorcentaje) {
      pgHandler.actualizaEstatusProcesoDatos(
        idBatch,
        CONSTANTS.BITACORAS_PASO_DESDOBLE,
        CONSTANTS.BITACORAS_ID_PROCESO,
        CatalogoEstatusProcesoDatos.Alerta,
        s"El porcentaje de cuarentena supera el ${ConfigurationProvider.identificadorPorcentajeCuarentenaPermitido}% de la recepción exitosa"
      )
    }

  }

  private def ejecutaFlujoNormal(insumosSize: Dataset[MetadataValidaSize], data: Map[String, String]): Unit = {
    logger.info("Inicio de flujo ======> 1")

    val particiones: Option[Int] = obtenerNumeroCoresEnCluster()
    val nuevoDf = insumosSize.filter($"tipoFlujo" <= 1)

    val dfProcesar = particiones match {
      case Some(value) =>
        logger.debug(s"Reparticionando dataframe lista fechas en bronce a $value particiones.")
        nuevoDf.repartition(value)
      case None => nuevoDf
    }

    val sas = ConfigurationProvider.sasBlobStorage

    dfProcesar
      .mapPartitions { partition =>
        val results = partition.map { r =>

          val string: Either[Throwable, String] = try {
            Right(new CloudBlockBlob(new URI(r.blobpath + sas)).downloadText())
          } catch {
            case e: Exception =>
              System.err.println(s"Error en ${r.blobpath}")
              Left(e)
          }
          val jsonToCaseClasses = new JsonToCaseClasses(data)

          val parserResults = string match {
            case Left(e) =>
              logger.error(e)
              jsonToCaseClasses.errorList.add(ErrorParser("ERROR_AL_DESCARGAR_BLOB", null, null, e.getMessage))
              Left(e)
            case Right(v) => jsonToCaseClasses.processSmallJson(v)
          }

          val bronceResult: Option[EsquemaDiot] = parserResults.toOption

          Bronce(
            r.numerooperacion.toLong,
            r.rfc,
            r.fechadeclaracion,
            r.id_ejecucion,
            r.blobpath,
            declaracionValida = jsonToCaseClasses.obtenerErrores.isEmpty,
            Util.obtenerTimestamp(),
            jsonToCaseClasses.obtenerErrores.asScala.toArray,
            bronceResult
          )
        }

        results
      }
      .withColumn("p_fechapresentacion", expr(ExpPartitionColumn))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tablaTemporalBronce)

    logger.info("Final de flujo ======> 1")
  }

  private def ejecutaFlujoArchivosGranVolumen(insumosSize: Dataset[MetadataValidaSize], data: Map[String, String]): Unit = {
    logger.info("Inicio de Flujo ======> 2")

    insumosSize
      .filter($"tipoFlujo" === 2)
      .collect()
      .foreach(row => {
        logger.info(s"numero operacion ======> ${row.numerooperacion}")
        val jsonToCaseClasses = new JsonToCaseClasses(data)
        val nombreCuenta = ConfigurationProvider.uriCuentaStorageArchivosJSON //new URL(row.blobpath).getHost.split("\\.").head
        val sas = ConfigurationProvider.sasBlobStorage
        var parserResults: Option[Seq[EsquemaDiot]] = None
        var fileInptuStreamSurf: BlobInputStream = null
        var fileInputStream: BlobInputStream = null
        val blobclient = new BlobContainerManager(row.blobpath.split("/").last, nombreCuenta, sas)

        try {

          fileInptuStreamSurf = blobclient
            .obtenerInputStreamParaBlobEnCuenta
            .get
          fileInputStream = blobclient
            .obtenerInputStreamParaBlobEnCuenta
            .get

          parserResults = Some(jsonToCaseClasses.processBigJson(fileInputStream, fileInptuStreamSurf))

        } catch {
          case e: BlobStorageException =>
            logger.error(e)
            jsonToCaseClasses.errorList.add(ErrorParser("ERROR_AL_DESCARGAR_BLOB", null, null, e.getMessage))
          case jsEx: JsonSurfingException =>
            logger.error(left(jsEx.getMessage, 5000))
            jsonToCaseClasses.errorList.add(ErrorParser("JSON MAL FORMADO", null, null, left(jsEx.getMessage, 5000)))
          case e: Exception =>
            logger.error(left(e.getMessage, 5000))
            jsonToCaseClasses.errorList.add(
              ErrorParser(
                "SIN RESULTADOS EN BRONCE",
                null,
                null,
                left(e.getMessage, 5000)
              )
            )
        }
        parserResults match {
          case Some(value) =>
            value
              .map { a =>
                Seq(
                  Bronce(
                    row.numerooperacion.toLong,
                    row.rfc,
                    row.fechadeclaracion,
                    row.id_ejecucion,
                    row.blobpath,
                    declaracionValida = jsonToCaseClasses.obtenerErrores.isEmpty,
                    Util.obtenerTimestamp(),
                    jsonToCaseClasses.obtenerErrores.asScala.toArray,
                    Some(a)
                  )
                )
                  .toDF()
                  .withColumn("p_fechapresentacion", expr(ExpPartitionColumn))
                  .write
                  .mode(SaveMode.Append)
                  .saveAsTable(tablaTemporalBronce)
              }

          case None =>
            Seq(
              Bronce(
                row.numerooperacion.toLong,
                row.rfc,
                row.fechadeclaracion,
                row.id_ejecucion,
                row.blobpath,
                declaracionValida = jsonToCaseClasses.obtenerErrores.isEmpty,
                Util.obtenerTimestamp(),
                jsonToCaseClasses.obtenerErrores.asScala.toArray,
                None
              )
            ).toDF()
              .withColumn("p_fechapresentacion", expr("to_date(date_trunc('MM', fechaPresentacion))"))
              .write
              .mode(SaveMode.Append)
              .saveAsTable(tablaTemporalBronce)
        }
      })
    logger.info("Final de Flujo ======> 2")
  }

  private def getJsonBlobSize(dfIngesta: Dataset[MetadataIngesta]): Unit = {

    spark.sql(s"DROP TABLE IF EXISTS $tablaSizeTemp")

    val limiteArchivosNormales = ConfigurationProvider.obtenerLimiteArchivosNormales
    val limiteArchivosGrandes = ConfigurationProvider.obtenerLimiteArchivosGrandes

    val broadcastStorageAccount = spark.sparkContext.broadcast(ConfigurationProvider.uriCuentaStorageArchivosJSON)
    val broadcastSAS = spark.sparkContext.broadcast(ConfigurationProvider.sasBlobStorage)

    dfIngesta.rdd.mapPartitions { partition =>
        partition.map { r =>
          val containerManager = new BlobContainerManager(r.blobpath.split("/").last, broadcastStorageAccount.value, broadcastSAS.value)
          var tipoFlujo: Integer = 0
          var size: Long = 0L
          if (r.blobpath != null && r.blobpath.nonEmpty) {
            try {
              val sizeFile = containerManager.getBlobSize
              sizeFile match {
                case Left(e) =>
                  logger.error(e)
                  size = 0L
                case Right(value) =>
                  size = value
                  tipoFlujo = if (value <= limiteArchivosNormales) 1
                  else if (value > limiteArchivosNormales && value <= limiteArchivosGrandes) 2
                  else 3
                  logger.info(s"============== el tamaño del archivo = $value")
              }
            } catch {
              case e: Exception =>
                logger.error(e.getMessage)
                size = 0L
            }
          }
          MetadataValidaSize(
            r.fechacarga,
            r.id_ejecucion,
            r.rfc,
            r.numerooperacion,
            r.obligaciones,
            r.fechadeclaracion,
            r.ejercicio,
            r.blobpath,
            size,
            tipoFlujo
          )
        }
      }
      .toDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .saveAsTable(tablaSizeTemp)
  }
}


