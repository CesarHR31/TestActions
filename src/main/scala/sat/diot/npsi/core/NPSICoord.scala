package sat.diot.npsi.core

import sat.diot.comunes.EnumsTablas.listaTablasOro
import sat.diot.comunes.Util
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.infraestructura.tablas.CifrasControlTable
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso
import sat.diot.npsi.core.GeneracionCSVNPSI._

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object NPSICoord {

  lazy val jdbcUrl: String = ConfigurationProvider.postgresqlControlUrl
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def procesaPendientes(): String = {
    var pendientes: String = ""
    postgresqlHandler
      .obtenerBatchConEstatus(CatalogoProceso.TERMINADO_CIFRAS_DETALLE_ORO_EXITOSAMENTE)
      .foreach { bi =>
        logger.info(s"Generando CSV para NPSI del batch ${bi.id_ejecucion}")
        generaNPSIRun(
          bi.id_ejecucion,
          bi.esReproceso
        ) match {
          case Left(e) =>
            Left(e)
          case Right(v) => pendientes = pendientes + v + ","
        }

      }


    pendientes.stripPrefix(",").stripSuffix(",").trim
  }

  def generaNPSIRun(idEjecucion: String, esReproceso: Boolean): Either[Throwable, String] = {

    import spark.implicits._

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("America/Mexico_City"))
    val fechaCsv = dateFormat.format(new Date)
    val nombreCsv = ConfigurationProvider.prefijoCsvDeltasOro + fechaCsv + "_" + idEjecucion
    val rutaInterna = ConfigurationProvider.mountCSVNPSI + "control_deltas_dec_diot_STG/"
    val rutaExterna = ConfigurationProvider.mountCSVNPSI + "control_deltas_dec_diot/"

    logger.info(s"Iniciando proceso de generacion de CSV para el proceso de NPSI")

    try {
      postgresqlHandler.actualizaIdEstatusBatch(idEjecucion, CatalogoProceso.PROCESANDOEXPORTACIONNPSI)

      val seqInfo = obtenerConteosDeTablas(listaTablasOro, s"id_ejecucion = '$idEjecucion'", idEjecucion)

      escribirDeltaCsv(seqInfo.toDF, rutaInterna, rutaExterna, nombreCsv)
      logger.info(s"Terminado proceso de generacion de CSV para NPSI")
      postgresqlHandler.actualizaIdEstatusBatch(idEjecucion, CatalogoProceso.TERMINADOEXPORTACIONNPSIEXITOSAMENTE)

      if (esReproceso) {
        logger.info(s"Se actualiza id_ejecucion con estatus de finalización del reproceso de la información")
        postgresqlHandler.actualizarEstatusReproceso(idEjecucion, 2, ConfigurationProvider.identificadorIdProceso)
      }

      Right(idEjecucion)

    } catch {
      case e: Exception => {
        logger.error(
          s"Ocurrio un error al tratar de crear el CSV para el NPSI en ${e.getClass.getCanonicalName}, error : ${e.getMessage}, Stack : ${
            e.getStackTrace
              .mkString("Array(", ", ", ")")
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
            CatalogoProceso.ERRORENEXPORTACIONNPSI,
            Util.obtenerTimestamp(),
            null,
            esReproceso,
            null,
            null,
            null
          )
        )
      }

        Left(e)
    }
  }

}
