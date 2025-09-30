package sat.diot.comunes

import org.apache.spark.sql.SparkSession

/**
 * Proporciona una instancia compartida de SparkSession para usarla en clases que la hereden.
 * Esta característica también hereda TLogging para proporcionar capacidades de registro.
 */
trait TSparkSession extends TLogging{
  protected lazy val spark: SparkSession = SparkSessionManager.session
}
