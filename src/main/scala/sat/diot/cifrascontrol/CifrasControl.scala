package sat.diot.cifrascontrol

import sat.diot.cifrascontrol.CountsConfiguration.countingInLayer
import sat.diot.cifrascontrol.TipoConteoCifras.TipoConteoCifras
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.TSparkSession
import sat.diot.entities.SparkQueryEntity
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.RegistroMonitoreoNPSI

import java.time.Duration
import java.time.temporal.ChronoUnit

object CifrasControl extends App with TSparkSession {
  private lazy val db = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )

  /**
   * Ejecuta el proceso de conteo de cifras sobre un conjunto de tablas configuradas para una etapa específica.
   * @param fechaInicial Fecha de inicio del rango de datos a considerar.
   * @param fechaFinal Fecha de fin del rango de datos a considerar.
   * @param idEjecucion Identificador único de la ejecución actual.
   * @param tipoConteoCifras Enumeración que define el tipo de conteo a realizar (bronce, plata u oro).
   */
  def obtenerCifras(fechaInicial: String, fechaFinal: String, idEjecucion: String, tipoConteoCifras: TipoConteoCifras): Unit = {
   /* Obtiene la configuración asociada al tipoConteoCifras mediante countingInLayer*/
    val config = countingInLayer(tipoConteoCifras)
    val targetTable = config.targetControlTable
    val typeControlCounts = config.targetTypeControlCounts
    val executionStep = config.executionStepId

    require(spark.catalog.tableExists(targetTable), "Tabla no autorizada.")

    /*Valida que exista al menos una tabla en la configuración.*/
    if (config.tablesSeq.nonEmpty) {
      config.tablesSeq.foreach { table =>
        require(spark.catalog.tableExists(table.name), "Tabla no autorizada.")

        val inputQuery = SparkQueryEntity(
          sourceTableName = table.name,
          initDate = fechaInicial,
          endDate = fechaFinal,
          executionId = idEjecucion,
          targetTableName = targetTable,
          typeCountingFigures = typeControlCounts,
          idTable = table.idTabla,
          executionStepId = executionStep
        )
        executeCounts(inputQuery)
      }
    }
    else {
      val error = s"El listado de tablas en $tipoConteoCifras se encuentra vacío."
      logger.error(error)
      throw new Exception(error)
    }
  }

  /**
   * Ejecuta el conteo y registra resultados en la tabla correspondiente en cada capa.
   * @param inputToQuery Entidad con parámetros necesarios para ejecutar la consulta de conteo de cifras.
   */
  private def executeCounts(inputToQuery: SparkQueryEntity): Unit = {
    val inicioEjecucion = System.currentTimeMillis
    try {
      val input = Map(inputToQuery.sourceTableName -> spark.table(inputToQuery.sourceTableName))

      val conteo = {
        ExecuteSparkQuery.executeQuery(input, inputToQuery)
      }
      db.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        inputToQuery.executionId,
        inputToQuery.executionStepId,
        inputToQuery.idTable,
        inputToQuery.sourceTableName,
        conteo,
        conteo,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
    } catch {
      case e: Exception =>
        db.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          inputToQuery.executionId,
          inputToQuery.executionStepId,
          inputToQuery.idTable,
          inputToQuery.sourceTableName,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString("Array(", ", ", ")")
        ))
        throw new Exception(e)
    }
  }
}
