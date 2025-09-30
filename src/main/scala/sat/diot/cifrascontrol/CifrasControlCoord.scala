package sat.diot.cifrascontrol

import sat.diot.cifrascontrol.TipoConteoCifras.TipoConteoCifras
import sat.diot.comunes.{CONSTANTS, CatalogoEstatusProcesoDatos, EnumControl, TSparkSession, Util}
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.CatalogoProceso
import sat.diot.infraestructura.tablas._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.matching.compat.RegexOps

class CifrasControlCoord extends TSparkSession {
  private val tiempoInicial = Util.obtenerTimestamp()
  private lazy val db = new PostgresqlHandler(ConfigurationProvider.postgresqlControlUrl)
  private val startSep = "Array("
  private val endSep = ")"
  private val separator = ", "

  def procesaPendientes(tipoConteoCifras: TipoConteoCifras): Unit = {
    var catalogoProceso = CatalogoProceso.TERMINADOPLATAEXITOSAMENTE

    tipoConteoCifras match {
      case TipoConteoCifras.bronce => catalogoProceso = CatalogoProceso.TERMINADOBRONCEEXITOSAMENTE
      case TipoConteoCifras.plata =>
        catalogoProceso = CatalogoProceso.TERMINADO_CONCILIACION_PLATA_EXITOSAMENTE
      case TipoConteoCifras.oro =>
        catalogoProceso = CatalogoProceso.TERMINADO_CONCILIACION_ORO_EXITOSAMENTE
    }

    db.obtenerBatchConEstatus(catalogoProceso)
      .foreach { bi =>
        logger.info(s"Procesando batch ${bi.id_ejecucion}")
        procesaBatch(bi.id_ejecucion, tipoConteoCifras)
      }
  }

  def procesaBatch(id: String, tipoConteoCifras: TipoConteoCifras): Unit = {
    val batchMetadata = db
      .obtenerMetadataBatch(id)
      .getOrElse(throw new NoSuchElementException(s"El batch con ID $id no existe."))

    val validDates = validateTimestamp(batchMetadata.fechaInicio, batchMetadata.fechaFin)

    tipoConteoCifras match {
      case TipoConteoCifras.bronce =>
        try {
          db.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDO_CIFRAS_DETALLE_BRONCE)
          CifrasControl.obtenerCifras(
            validDates._1,
            validDates._2,
            batchMetadata.id_ejecucion,
            tipoConteoCifras
          )
          db.actualizarEstatusBatch(
            id,
            CatalogoProceso.TERMINADO_CIFRAS_DETALLE_BRONCE_EXITOSAMENTE
          )
        } catch {
          case e: Exception =>
            logger.error(
              s"Error al ejecutar cifras de control detalle de ${tipoConteoCifras.toString}: " + e.getMessage + "|" + e.getStackTrace
                .mkString(startSep, separator, endSep)
            )
            db.actualizarEstatusBatch(id, CatalogoProceso.ERROR_CIFRAS_DETALLE_BRONCE)
        }

      case TipoConteoCifras.plata =>
        try {
          db.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDO_CIFRAS_DETALLE_PLATA)
          CifrasControl.obtenerCifras(
            validDates._1,
            validDates._2,
            batchMetadata.id_ejecucion,
            tipoConteoCifras
          )
          db.actualizarEstatusBatch(
            id,
            CatalogoProceso.TERMINADO_CIFRAS_DETALLE_PLATA_EXITOSAMENTE
          )

          db.insertaBitacoraProcesoDatos(BitacoraProcesoDatos(
            id,
            CONSTANTS.BITACORAS_ID_PROCESO,
            CONSTANTS.BITACORAS_PASO_PLATA,
            CatalogoEstatusProcesoDatos.Exito,
            None, spark.sql(
              s"""SELECT IF(SUM(noregistros) IS NULL,0, SUM(noregistros)) contadorRegistros
                   FROM ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasPlata).name} WHERE id_ejecucion = '$id'""").head.getLong(0).toInt,
            tiempoInicial,
            Util.obtenerTimestamp(),
            batchMetadata.fechaInicio,
            batchMetadata.fechaFin, null, null
          ))

          //RN-03 Si el contador de diot_plata.datosidentideclaracion es diferente a la recepción exitosa el registro de plata se guarda
          // con estatus de alerta, en la descripción colocar el texto: “Los registros de la tabla encabezado no corresponden con la recepción exitosa ”
          val concuerdanCifras = spark.sql(
            s"""SELECT IF(A.nodeclaracionesdesdobladas = B.noregistros, true, false) concuerdan
                  FROM ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasBronce).name} A
                  INNER JOIN ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasPlata).name} B
                    ON A.id_ejecucion = B.id_ejecucion
                  WHERE A.id_ejecucion = '$id' AND lower(B.tabla) = lower('${ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name}')""").head.getBoolean(0)

          if (!concuerdanCifras) {
            db.actualizaEstatusProcesoDatos(
              id,
              CONSTANTS.BITACORAS_PASO_PLATA,
              CONSTANTS.BITACORAS_ID_PROCESO,
              CatalogoEstatusProcesoDatos.Alerta,
              "Los registros de la tabla encabezado no corresponden con la recepción exitosa")
          }

        } catch {
          case e: Exception =>
            logger.error(
              s"Error al ejecutar cifras de control detalle de ${tipoConteoCifras.toString}: " + e.getMessage + "|" + e.getStackTrace
                .mkString(startSep, separator, endSep)
            )
            db.actualizarEstatusBatch(id, CatalogoProceso.ERROR_CIFRAS_DETALLE_PLATA)
        }

      case TipoConteoCifras.oro =>
        try {
          db.actualizarEstatusBatch(id, CatalogoProceso.PROCESANDO_CIFRAS_DETALLE_ORO)
          CifrasControl.obtenerCifras(
            validDates._1,
            validDates._2,
            batchMetadata.id_ejecucion,
            tipoConteoCifras
          )
          db.actualizarEstatusBatch(
            id,
            CatalogoProceso.TERMINADO_CIFRAS_DETALLE_ORO_EXITOSAMENTE
          )
          db.insertaBitacoraProcesoDatos(BitacoraProcesoDatos(
            id,
            CONSTANTS.BITACORAS_ID_PROCESO,
            CONSTANTS.BITACORAS_PASO_ORO,
            CatalogoEstatusProcesoDatos.Exito,
            None, spark.sql(
              s"""SELECT IF(SUM(noregistros) IS NULL,0, SUM(noregistros)) contadorRegistros
                   FROM ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasOro).name} WHERE id_ejecucion = '$id'""".stripMargin).head.getLong(0).toInt,
            tiempoInicial,
            Util.obtenerTimestamp(),
            batchMetadata.fechaInicio,
            batchMetadata.fechaFin, null, null
          ))
        } catch {
          case e: Exception =>
            logger.error(
              s"Error al ejecutar cifras de control detalle de ${tipoConteoCifras.toString}: " + e.getMessage + "|" + e.getStackTrace
                .mkString(startSep, separator, endSep)
            )
            db.actualizarEstatusBatch(id, CatalogoProceso.ERROR_CIFRAS_DETALLE_ORO)
        }

    }
  }

  private def validateTimestamp(initDate: Timestamp, endDate: Timestamp): (String, String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datePattern = """^(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}).*$""".r
    require(datePattern.matches(initDate.toString), "Formato de fecha inválido")
    require(datePattern.matches(endDate.toString), "Formato de fecha inválido")

    (format.format(initDate), format.format(endDate))
  }
}
