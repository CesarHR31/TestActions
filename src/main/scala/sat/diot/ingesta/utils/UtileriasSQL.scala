package sat.diot.ingesta.utils

import org.apache.spark.sql.SparkSession
import sat.diot.comunes.SparkSessionManager
import slick.util.Logging

object UtileriasSQL extends Logging {
  private def spark: SparkSession = SparkSessionManager.session

  val spark2 = spark
  import spark2.implicits._

  def eliminarRegistrosDeTablaPorNumeroOperacion(
    nombreTabla: String,
    filtrosEliminacion: (List[Long], List[String])
  ): Unit = {

    if (spark.catalog.tableExists("default.diot_tablaTempNumOps"))
      spark.sql(s"DROP TABLE default.diot_tablaTempNumOps")

    filtrosEliminacion._1.toDF("numerooperacion")
      .write
      .format("delta")
      .mode("append")
      .saveAsTable("default.diot_tablaTempNumOps")

    if (filtrosEliminacion != null && filtrosEliminacion._1.nonEmpty && filtrosEliminacion._2.nonEmpty) {

      filtrosEliminacion._2.foreach(filtro => {
        //https://github.com/delta-io/delta/issues/730 No probar si e local, solo funciona en cluster
        spark.sql(s"""
                            DELETE FROM $nombreTabla A
                                   WHERE A.p_fechapresentacion == date_trunc('MM', "${filtro}")
                                   AND EXISTS
                                   (SELECT 1 FROM default.diot_tablaTempNumOps B
                                   WHERE A.numerooperacion = B.numerooperacion
                                   )
                                   """)
      })

    }
  }
}
