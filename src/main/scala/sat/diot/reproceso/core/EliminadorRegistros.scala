package sat.diot.reproceso.core

import io.delta.tables.DeltaTable
import sat.diot.comunes.SparkSessionManager
import sat.diot.comunes.config.ConfigurationProvider

import scala.util.control.NonFatal

class EliminadorRegistros {

  private lazy val spark = SparkSessionManager.session
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  import spark.implicits._

  def eliminarNumerosOperacion(
    numOps: Seq[String]
  ): Map[String, Boolean] = {

    val gConfigs = ConfigurationProvider.configs

    Seq(
      gConfigs.identificadorBaseLand,
      gConfigs.identificadorBaseBronce,
      gConfigs.identificadorBasePlata,
      gConfigs.identificadorBaseOro
    ).flatMap { db =>
      eliminarNumerosOperacion(numOps, db)
    }.toMap
  }

  def eliminarNumerosOperacion(numOps: Seq[String], database: String): Map[String, Boolean] = {
    spark.catalog
      .listTables(database)
      .collect()
      .map { t =>
        val fullyQualifiedName = s"${database}.${t.name}"
        val result =
          try {
            logger.info(s"Eliminando registros de $fullyQualifiedName")
            DeltaTable
              .forName(fullyQualifiedName)
              .delete($"numerooperacion".isin(numOps: _*))
            true
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error eliminando registros de $fullyQualifiedName", e)
              false
          }

        (fullyQualifiedName -> result)
      }
      .toMap
  }
}
