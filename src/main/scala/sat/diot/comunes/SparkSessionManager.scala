package sat.diot.comunes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionManager extends Serializable {

  private lazy val sparkConf: SparkConf = new SparkConf(false)
    .setAppName("DIOT")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.shuffle.partitions", "2")
    .set("spark.sql.session.timeZone", "UTC")
    .setMaster("local[*]")

  private var sparkSession: SparkSession = _
  var localSpark = false

  def getOrCreateLocal: SparkSession = {
    localSpark = true
    session
  }

  def session: SparkSession =
    this.synchronized {
      if (localSpark) {
        local.sparkContext.setLogLevel("ERROR")
        local
      } else {
        provided
      }
    }

  //https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake
  private def local = {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder
        .config(sparkConf)
        .getOrCreate()
    }

    if (sparkSession.sparkContext.isStopped) {
      sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    }

    sparkSession
  }

  private def provided =
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .getOrCreate()
      sparkSession
    } else {
      sparkSession
    }

}
