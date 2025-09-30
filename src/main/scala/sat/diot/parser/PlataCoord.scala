package sat.diot.parser

import org.apache.spark.sql.functions._
import sat.diot.comunes.SparkSessionManager
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso

class PlataCoord {
  private lazy val sampleFilesBasePathPlata = ConfigurationProvider.pathScriptsLlenadoPlata
  private lazy val db = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def procesaPendientes(): Unit = {
    db.obtenerBatchConEstatus(CatalogoProceso.TERMINADO_CIFRAS_DETALLE_BRONCE_EXITOSAMENTE)
      .foreach { bi =>
        logger.info(s"Procesando batch ${bi.id_ejecucion}")
        procesaBatch(bi.id_ejecucion)
      }
  }

  def procesaBatch(id: String): Unit = {
    val batchMetadata = db
      .obtenerMetadataBatch(id)
      .getOrElse(throw new NoSuchElementException(s"El batch con ID $id no existe."))

    db.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDOPLATA)

    new EjecutorScripts(sampleFilesBasePathPlata)
      .procesarTablas(
        batchMetadata.id_ejecucion,
        List(0L),
        batchMetadata.fechaInicio.toString,
        batchMetadata.fechaFin.toString,
        batchMetadata.esReproceso
      )

    val expresionFiltros =
      s"""p_fechapresentacion BETWEEN
         | date_trunc('MM', '${batchMetadata.fechaInicio}')
         | AND date_trunc('MM', '${batchMetadata.fechaFin}')""".stripMargin.replaceAll("\n", "")

    logger.info(s"Filtrando plata con la expresión $expresionFiltros")

    val batchRows = spark
      .table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name)
      .filter(
        col("id_ejecucion") === lit(id) &&
          expr(expresionFiltros)
      )
      .count

    db.actualizarEstatusBatch(id, CatalogoProceso.TERMINADOPLATAEXITOSAMENTE)
    db.actualizaCifrasControlPlata(id, batchRows)

  }

  private def spark = SparkSessionManager.session
}
