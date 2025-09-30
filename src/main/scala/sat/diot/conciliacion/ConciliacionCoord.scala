package sat.diot.conciliacion

import sat.diot.comunes.{EnumBronce, EnumOro}
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso
import sat.diot.conciliacion.TipoConciliacion._
import sat.diot.conciliacion.Conciliacion.procesaConciliacion

import java.text.SimpleDateFormat

class ConciliacionCoord {
  private lazy val db = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def procesaPendientes(tipoConciliacion: TipoConciliacion): Unit = {
    var catalogoProceso = CatalogoProceso.TERMINADOPLATAEXITOSAMENTE
    if (tipoConciliacion == TipoConciliacion.oro)
      catalogoProceso = CatalogoProceso.TERMINADOOROEXITOSAMENTE
    db.obtenerBatchConEstatus(catalogoProceso)
      .foreach { bi =>
        logger.info(s"Procesando batch ${bi.id_ejecucion}")
        procesaBatch(bi.id_ejecucion, tipoConciliacion)
      }
  }

  def procesaBatch(id: String, tipoConciliacion: TipoConciliacion): Unit = {
    val batchMetadata = db
      .obtenerMetadataBatch(id)
      .getOrElse(throw new NoSuchElementException(s"El batch con ID $id no existe."))

    if (tipoConciliacion == TipoConciliacion.plata) {
      db.actualizaIdEstatusBatch(id, CatalogoProceso.PROCESANDO_CONCILIACION_PLATA)
      logger.info(s"Procesando conciliación plata")
    } else {
      db.actualizaIdEstatusBatch(id, CatalogoProceso.PROCESANDO_CONCILIACION_ORO)
      logger.info(s"Procesando conciliación plata")
    }
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var tablaBase = ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name
    var tablaAConciliar = ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name
    if (tipoConciliacion == TipoConciliacion.oro) {
      tablaBase = ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name
      tablaAConciliar = ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name
    }
    val resultadoConciliacion =

      procesaConciliacion(EntidadProcesoConciliacion(
        format.format(batchMetadata.fechaInicio),
        format.format(batchMetadata.fechaFin),
        tablaBase,
        tablaAConciliar,
        tipoConciliacion,
        id
      ))

    resultadoConciliacion.either match {
      case Left(e) =>
        if (tipoConciliacion == TipoConciliacion.plata) {
          db.actualizarEstatusBatch(id, CatalogoProceso.ERROR_CONCILIACION_PLATA)
          logger.error(s"Error conciliación plata ${e.getStackTrace.mkString("Array(", ", ", ")")}")
        } else {
          db.actualizarEstatusBatch(id, CatalogoProceso.ERROR_CONCILIACION_ORO)
          logger.error(s"Error conciliación oro ${e.getStackTrace.mkString("Array(", ", ", ")")}")
        }
      case Right(value) =>
        if (tipoConciliacion == TipoConciliacion.plata) {
          db.actualizarEstatusBatch(id, CatalogoProceso.TERMINADO_CONCILIACION_PLATA_EXITOSAMENTE)
          db.actualizaCifrasControlPlata(id, value._2)
          logger.info(s"Terminada conciliación plata")
        } else {
          db.actualizarEstatusBatch(id, CatalogoProceso.TERMINADO_CONCILIACION_ORO_EXITOSAMENTE)
          db.actualizaCifrasControlOro(id, value._2)
          logger.info(s"Terminada conciliación oro")
        }
    }
  }
  //private def spark: SparkSession = SparkSessionManager.session
}
