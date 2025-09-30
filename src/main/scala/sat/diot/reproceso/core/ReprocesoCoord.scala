package sat.diot.reproceso.core

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import sat.diot.comunes.Util
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas.CifrasControlTable
import sat.diot.ingesta.core.ProcesamientoCapas.creacionDFLand
import sat.diot.ingesta.entities.CatalogoProceso
import sat.diot.reproceso.core.ProcesamientoDatos.{creacionLand, recuperarListado}
import sat.diot.reproceso.utils.AzureBlobs
import slick.util.Logging

object ReprocesoCoord extends Logging {

  lazy val jdbcUrl: String   = ConfigurationProvider.postgresqlControlUrl
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)

  def reprocesoRun(id_ejecucion: String, nombreBlob: String): Either[Throwable, Double] = {
    logger.info("Comenzando reproceso")
    postgresqlHandler.insertaHistorico((id_ejecucion))
    val tiempoInicio = System.nanoTime

    postgresqlHandler.insertaCifrasControl(id_ejecucion)
    logger.info(s"Cargando configuraciones de Cuenta de storage")

    try {

      val storageAzure =
        new AzureBlobs(dbutils.secrets.get("diot-uat", "azure-conn-string-aplicativo-uat"), "reprocesosuat")

      val resultado = recuperarListado(storageAzure, nombreBlob)
      logger.info(s"Cargado listado de archivos historico")

      val resultadoLand = creacionLand(resultado, id_ejecucion)
      logger.info(s"Terminado creacion de modelo Land")

      val tablaLandDF = creacionDFLand(resultadoLand, id_ejecucion, null)
      logger.info(s"Terminado creacion de tabla Land")
//
      postgresqlHandler.actualizaCifrasControlLand(
        CifrasControlTable(
          id_ejecucion,
          null,
          null,
          tablaLandDF.count(),
          0L,
          0L,
          0L,
          0L,
          0L,
          Util.obtenerTimestamp(),
          CatalogoProceso.TERMINADOLANDEXITOSAMENTE,
          Util.obtenerTimestamp(),
          null,
          false,
          null,
          null,
          null
        )
      )

      logger.info(s"Terminada insercion de registros en cifras de control")

      Right((System.nanoTime - tiempoInicio) / 1e9d)
    } catch {
      case e: Exception => {
          logger.error(
            s"Ocurrio un error al tratar de procesar en ${e.getClass.getCanonicalName}, error : ${e.getMessage}, Stack : ${e.getStackTrace
              .mkString("Array(", ", ", ")")}"
          )
          postgresqlHandler.actualizaEstadoBatch(
            CifrasControlTable(
              id_ejecucion,
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
              false,
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
