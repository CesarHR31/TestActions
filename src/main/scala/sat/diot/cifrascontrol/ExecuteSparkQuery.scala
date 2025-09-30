package sat.diot.cifrascontrol

import org.apache.spark.sql.functions.{count, countDistinct, current_timestamp, date_trunc, from_utc_timestamp, lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode}
import sat.diot.comunes.TSparkSession
import sat.diot.entities.SparkQueryEntity

import scala.util.Try

object ExecuteSparkQuery extends TSparkQuery with TSparkSession {

  /**
   * Columnas que se agregan a la(s) tabla(s) final(es) de cifras para cada etapa (bronce, plata, oro).
   */
  private final val COLUMNS_TO_ADD: String = "fechainsercion|tabla"
  /**
   * Columna que se elimina del dataframe obtenido de la consulta en bronce.
   */
  private final val COLUMN_TO_DROP: String = "payload"

  import spark.implicits._

  /**
   * Elimina columna del dataframe obtenido de la consulta a bronce.
   *
   * @param df DataFrame de la consulta a la tabla en bronce.
   * @return Devuelve nuevo dataframe si la columna eliminada.
   */
  override protected def dropColumnFromDf(df: DataFrame): DataFrame = {
    df.drop(COLUMN_TO_DROP)
  }

  /**
   * Agrega columnas fechainsercion y p_fechainsercion al DF.
   *
   * @param df DataFrame filtrado por fechaPresentacion e id_ejecucion.
   * @return Devuelve nuevo DataFrame con las nuevas columnas.
   */
  override protected def addColumnToDf(df: DataFrame): DataFrame = {
    val columnInsertionDate = COLUMNS_TO_ADD.split("\\|")(0)
    df.withColumn(columnInsertionDate, from_utc_timestamp(current_timestamp, "America/Mexico_City"))
      .withColumn(s"p_$columnInsertionDate", date_trunc("MM", current_timestamp))
  }

  /**
   * Aplica filtro por id_ejecucion.
   *
   * @param df DataFrame Filtrado por fechaPresentacion.
   * @return Devuelve DataFrame filtrado por id_ejecucion.
   */
  override protected def filterByExecutionId(df: DataFrame): DataFrame = {
    df.where($"id_ejecucion" === sparkQueryEntity.executionId)
  }

  /**
   * Aplica filtro a DataFrame por fechaPresentacion.
   *
   * @param df DataFrame obtenido de la consulta de la tabla fuente (bronce).
   * @return Devuelve DataFrame filtrado por fechaPresentacion.
   */
  override protected def filterByDateTime(df: DataFrame): DataFrame = {
    df.where($"fechaPresentacion" >= sparkQueryEntity.initDate
      && $"fechaPresentacion" <= sparkQueryEntity.endDate)
  }

  /**
   * Aplica filtro por el campo partición p_fechaPresentacion
   *
   * @param df DataFrame obtenido de la consulta de la tabla fuente (bronce).
   * @return Devuelve DataFrame filtrado por el campo partición.
   */
  override protected def filterByDateTrunc(df: DataFrame): DataFrame = {
    df.where($"p_fechaPresentacion" >= date_trunc("MM", lit(sparkQueryEntity.initDate))
      && $"p_fechaPresentacion" <= date_trunc("MM", lit(sparkQueryEntity.endDate)))
  }

  /**
   * Obtiene conteos basado en la tabla fuente (bronce).
   *
   * @param df DataFrame filtrado por fechaPresentacion e id_ejecucion.
   * @return Devuelve DataFrame con cifras para insertar en la tabla final cifrascontrolbronce.
   */
  override protected def getBronzeCounts(df: DataFrame): DataFrame = {
    df.groupBy("id_ejecucion")
      .agg(count($"numeroOperacion") as "nodeclaraciones",
        count(when($"declaracionValida" === true, 1)) as "nodeclaracionesdesdobladas",
        count(when(!$"declaracionValida" === true, 1)) as "nodeclaracionescuarentena")
  }

  /**
   * Obtiene conteos por tabla en la etapa plata.
   *
   * @param df DataFrame filtrado por fechaPresentacion e id_ejecucion.
   * @return Devuelve DataFrame con cifras para insertar en la tabla final cifrascontrolplata.
   */
  override protected def getSilverGoldCounts(df: DataFrame): DataFrame = {
    val nameColumn = COLUMNS_TO_ADD.split("\\|")(1)

    df.withColumn(nameColumn, lit(sparkQueryEntity.sourceTableName))
      .groupBy("id_ejecucion", nameColumn)
      .agg(countDistinct($"numeroOperacion") as "nodeclaraciones",
        count("numeroOperacion") as "noregistros")
  }

  /**
   * Escribe DataFrame con las transformaciones aplicadas en la tabla destino correspondiente.
   *
   * @param df                  DataFrame a escribir.
   * @return Devuelve conteo de registros por etapa por tabla.
   */
  override protected def writeInTargetTable(df: DataFrame): Long = {
    df.write
      .mode(SaveMode.Append)
      .format("delta")
      .saveAsTable(sparkQueryEntity.targetTableName)

    if (sparkQueryEntity.typeCountingFigures != TipoConteoCifras.bronce) Try(df.head.getLong(3)).getOrElse(0) else df.count
  }

  /**
   * Ejecuta transformaciones a DataFrame origen (bronce, plata, oro).
   *
   * @param inputsToDataFrame   Tupla con identificador del DF origen y DF en si.
   * @param inputSparkQuery     Entidad con parámetros para la ejecución de las consultas.
   */
  override def executeQuery(inputsToDataFrame: Map[String, DataFrame], inputSparkQuery: SparkQueryEntity): Long = {

    sparkQueryEntity = inputSparkQuery

    var resultDF = inputsToDataFrame(sparkQueryEntity.sourceTableName)
      .transform(dropColumnFromDf)
      .transform(filterByDateTime)
      .transform(filterByExecutionId)

    if (inputSparkQuery.typeCountingFigures != TipoConteoCifras.bronce) {
      resultDF = resultDF
        .transform(getSilverGoldCounts)
        .transform(addColumnToDf)
    }
    else {
      resultDF = resultDF.transform(getBronzeCounts)
        .transform(addColumnToDf)
    }

    writeInTargetTable(resultDF)
  }
}