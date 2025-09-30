package sat.diot.parser

import org.apache.spark.sql.functions._
import sat.diot.comunes.{EnumLand, SparkSessionManager}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso

import java.text.SimpleDateFormat

class BronceCoord {

  private lazy val postgresqlHandler = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )

  private lazy val spark = SparkSessionManager.session
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  import spark.implicits._

  def procesaPendientes(): Unit = {
    postgresqlHandler
      .obtenerBatchConEstatus(CatalogoProceso.TERMINADOLANDEXITOSAMENTE)
      .foreach { bi =>
        logger.info(s"Procesando batch ${bi.id_ejecucion}")
        procesaBatch(bi.id_ejecucion)
      }
  }

  def procesaBatch(id: String): Unit = {
    val batchMetadata = postgresqlHandler
      .obtenerMetadataBatch(id)
      .getOrElse(throw new NoSuchElementException(s"El batch con ID $id no existe."))

    val dateFormat        = new SimpleDateFormat("yyyy-MM-dd")
    val fechaInicioFiltro = dateFormat.format(batchMetadata.fechaInicio)
    val fechaFinFiltro    = dateFormat.format(batchMetadata.fechaFin)
    val columnaParticion  = "p_fechapresentacion"

    val expresionFiltros =
      s"""$columnaParticion BETWEEN
         | date_trunc('MM', '${batchMetadata.fechaInicio}')
         | AND date_trunc('MM', '${batchMetadata.fechaFin}')""".stripMargin.replaceAll("\n", "")

    logger.debug(expresionFiltros)

    logger.info(
      s"""Buscando registros en land con el id_ejecucion ${batchMetadata.id_ejecucion}
         | y en particion $columnaParticion con fecha inicial $fechaInicioFiltro 
         | y fecha final $fechaFinFiltro""".stripMargin
        .replaceAll("\n", "")
    )

    val dataLand = spark
      .table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
      .where(
        $"id_ejecucion" === lit(batchMetadata.id_ejecucion)
          && expr(expresionFiltros)
      )
      .as[MetadataIngesta]

    if (dataLand.count() == 0) {
      postgresqlHandler.actualizarEstatusBatch(id, CatalogoProceso.SINDECLARACIONESBRONCE)
    } else {
      postgresqlHandler.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDOBRONCE)
      //CAMBIAR REFERENCIA
      new ParserDiot().procesar(dataLand, batchMetadata.fechaInicio, batchMetadata.fechaFin)
    }

  }

}
