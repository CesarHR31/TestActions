package sat.diot.cifrascontrol

import org.apache.spark.sql.DataFrame
import sat.diot.entities.SparkQueryEntity

trait TSparkQuery {
  protected var sparkQueryEntity: SparkQueryEntity = _

  protected def dropColumnFromDf(df: DataFrame): DataFrame

  protected def addColumnToDf(df: DataFrame): DataFrame

  protected def filterByExecutionId(df: DataFrame): DataFrame

  protected def filterByDateTime(df: DataFrame): DataFrame

  protected def filterByDateTrunc(df: DataFrame): DataFrame

  protected def getBronzeCounts(df: DataFrame): DataFrame

  protected def getSilverGoldCounts(df: DataFrame): DataFrame

  protected def writeInTargetTable(df: DataFrame): Long

  protected def executeQuery(inputsToDataFrame: Map[String, DataFrame], inputSparkQuery: SparkQueryEntity): Long
}
