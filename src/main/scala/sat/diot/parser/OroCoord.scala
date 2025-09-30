package sat.diot.parser

import org.apache.spark.sql.functions._
import sat.diot.comunes.{EnumOro, SparkSessionManager}
import sat.diot.comunes.config.{ConfigurationProvider, EnumDefault}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso

class OroCoord {
  private lazy val sampleFilesBasePath = ConfigurationProvider.pathScriptsLlenadoOro
  private lazy val spark = SparkSessionManager.session
  private lazy val db = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def procesaPendientes(): Unit = {
    db.obtenerBatchConEstatus(CatalogoProceso.TERMINADO_CIFRAS_DETALLE_PLATA_EXITOSAMENTE)
      .foreach { bi =>
        logger.info(s"Procesando batch ${bi.id_ejecucion}")
        procesaBatch(bi.id_ejecucion)
      }
  }

  def procesaBatch(id: String): Unit = {
    val batchMetadata = db
      .obtenerMetadataBatch(id)
      .getOrElse(throw new NoSuchElementException(s"El batch con ID $id no existe."))

    db.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDOORO)

    new EjecutorScripts(sampleFilesBasePath)
      .procesarTablas(
        batchMetadata.id_ejecucion,
        List(0L),
        batchMetadata.fechaInicio.toString,
        batchMetadata.fechaFin.toString,
        batchMetadata.esReproceso
      )

    val batchRows = spark
      .table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name)
      .filter(
        col("id_ejecucion") === lit(id) &&
          expr(
            s"""p_fechapresentacion BETWEEN
               |date_trunc('MM', '${batchMetadata.fechaInicio}')
               |AND date_trunc('MM', '${batchMetadata.fechaFin}')""".stripMargin)
      )
      .count

    db.actualizarEstatusBatch(id, CatalogoProceso.TERMINADOOROEXITOSAMENTE)
    db.actualizaCifrasControlOro(id, batchRows)

  }
}
