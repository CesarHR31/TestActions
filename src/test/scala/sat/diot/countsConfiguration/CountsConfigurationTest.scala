package sat.diot.countsConfiguration

import org.scalatest.funsuite.AnyFunSuite
import sat.diot.cifrascontrol.CountsConfiguration.countingInLayer
import sat.diot.cifrascontrol.TipoConteoCifras
import sat.diot.comunes.TSparkSession
import sat.diot.countsConfiguration.ConstantsAllowTables.bronzeSeq
import sat.diot.entities.SparkQueryEntity

class CountsConfigurationTest extends AnyFunSuite with TSparkSession {

  test("Get Configuration For Bronze") {
    val configs = countingInLayer(TipoConteoCifras.bronce)
    val targetTable = configs.targetControlTable
    val typeControlCounts = configs.targetTypeControlCounts
    val executionStep = configs.executionStepId

    /*Valida que exista al menos una coincidencia con el valor de la variable `targetTable`*/
    require(bronzeSeq.exists(targetTable.contains), "Tabla no autorizada.")

    if (configs.tablesSeq.nonEmpty) {
      configs.tablesSeq.foreach { table =>
        //require(spark.catalog.tableExists(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name), logger.error("Tabla no autorizada."))
        require(bronzeSeq.exists(table.name.contains), s"Tabla no autorizada.")

        val inputQuery = SparkQueryEntity(
          sourceTableName = table.name,
          initDate = "fechaInicial",
          endDate = "fechaFinal",
          executionId = "idEjecucion",
          targetTableName = targetTable,
          typeCountingFigures = typeControlCounts,
          idTable = table.idTabla,
          executionStepId = executionStep
        )
        println(s"Ejecutando query con los siguientes parámetros: ${inputQuery.sourceTableName}\t${inputQuery.targetTableName}\t${inputQuery.idTable}\t${inputQuery.executionStepId}")
      }
    }
    else {
      val error = s"El listado de tablas en $typeControlCounts se encuentra vacío."
      logger.error(error)
      throw new Exception(error)
    }
  }
}

object ConstantsAllowTables {
  val bronzeSeq = Set("bronce_diot", "cifrascontrolbronce")
  val silverSeq = Set("diot_decinfopeter_plata", "diot_infterdetiva_plata", "diot_inftotimpiva_plata")
}
