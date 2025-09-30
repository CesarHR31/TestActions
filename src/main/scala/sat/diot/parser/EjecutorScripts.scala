package sat.diot.parser

import org.apache.spark.sql.SparkSession
import sat.diot.comunes.config.{ConfigurationProvider, EnumDefault, EnumPlata}
import sat.diot.comunes._

import java.io.File
import scala.util.control.NonFatal

class EjecutorScripts(filesPath: String) {

  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)


  def procesarTablas(
                      batchId: String,
                      listaNumeroOperacion: List[Long],
                      fechaInicial: String,
                      fechaFinal: String,
                      esReproceso: Boolean
                    ): Unit = {
    new File(filesPath)
      .listFiles()
      .filter(p => !p.getAbsolutePath.toLowerCase().contains("vigente")) //--> Solo va por los que no son de vigentes
      .foreach { file =>
        logger.info(file.getAbsolutePath)

        val operacion: String = "I"

        val partitionFilter: String = if (!esReproceso) {
          s"AND p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial') AND date_trunc('MM', '$fechaFinal')"
        } else {
          ""
        }

        val string = Util
          .getFileContents(file.getAbsolutePath).replace("\uFEFF", "")
          .replace("$IDBATCH$", batchId)
          .replace("$NUMEROSOPERACION$", listaNumeroOperacion.mkString(","))

          //----------------------------------- ORO
          .replace("$diot_decinfopeter$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name)
          .replace("$diot_infterdetiva$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_infterdetiva).name)
          .replace("$diot_inftotimpiva$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_inftotimpiva).name)

          //------------------------- PLATA
          .replace("$plata_diot_decinfopeter$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name)
          .replace("$plata_diot_infterdetiva$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_infterdetiva).name)
          .replace("$plata_diot_inftotimpiva$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_inftotimpiva).name)

          //-----------------------
          .replace("$bronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
          .replace("$vistacuarentena$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name)
          .replace("$vistabronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name)
          .replace("$cifrascontrolbronce$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasBronce).name)
          .replace("$cifrascontroloro$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasOro).name)
          .replace("$cifrascontrolplata$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasPlata).name)
          .replace("$conciliacion$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaConciliacion).name)
          .replace("$land$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
          .replace("$PARTITION_FILTER$", partitionFilter)
          .replace("$operacion$", operacion)
          .replace("$ultimavigenteland$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
          .replace("$ultimavigentebronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name)
          .replace("$ultimavigenteplata$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name)
          .replace("$ultimavigenteoro$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)

        try {
           spark.sql(string)
        } catch {
          case NonFatal(e) => {
            println(s"%%%%%%%%%%%%%%%%%%%%% sql $string")
            println(s"-----> $e")
            logger.error(s"Ocurrio un error al cargar oro, error", e)
          }
            logger.info(string)

        }
      }
  }

  private def spark: SparkSession = SparkSessionManager.session

  def procesarTablas(
                      idejecucion: String,
                      fechaInicial: String,
                      fechaFinal: String,
                    ): Unit = {

    val partitionFilter: String = s"(p_timestamp BETWEEN date_trunc('MM', '$fechaInicial') AND date_trunc('MM', '$fechaFinal')) AND (timestamp BETWEEN '$fechaInicial' AND '$fechaFinal')"

    new File(filesPath)
      .listFiles()
      .filter(p => p.getAbsolutePath.toLowerCase().contains("vigente")) //--> Solo va por los vigentes
      .foreach { file =>
        val string = Util
          .getFileContents(file.getAbsolutePath)
          .replace("$IDBATCH$", idejecucion)
          .replace("$PARTITION_FILTER$", partitionFilter)
          .replace("$ultimavigenteland$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
          .replace("$ultimavigentebronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name)
          .replace("$ultimavigenteplata$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name)
          .replace("$ultimavigenteoro$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)
          .replace("$controlultimavigente$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name)
        try {
          spark.sql(string)
        } catch {
          case NonFatal(e) => {
            println(s"%%%%%%%%%%%%%%%%%%%%% sql $string")
            println(s"-----> $e")
            logger.error(s"Ocurrio un error", e)
          }
            logger.info(string)
        }
      }
  }
}
