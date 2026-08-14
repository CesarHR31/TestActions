//test comment 2
package comunes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionManager extends Serializable {

  private lazy val sparkConf = new SparkConf(false)
    .setAppName("GithubActions")
//    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.session.timeZone", "UTC")
    .setMaster("local[*]")

  private var sparkSession: SparkSession = _
  var localSpark                 = false

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

  private def local: SparkSession = {
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

  private def provided: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .getOrCreate()
      sparkSession
    } else {
      sparkSession
    }
  }

}
