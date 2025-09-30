package sat.diot.comunes

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.util.Try

object DatabricksUtils {

  private lazy val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)

  def obtenerContextoActual(): String = {
    Try(jsonMapper.writeValueAsString(dbutils.notebook.getContext().tags))
      .getOrElse(null)
  }

}
