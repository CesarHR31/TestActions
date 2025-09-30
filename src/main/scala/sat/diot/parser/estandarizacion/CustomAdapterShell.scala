package sat.diot.parser.estandarizacion

import com.google.gson.TypeAdapter
import com.google.gson.stream._
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.parser.{ErrorParser, UtileriasParser}

abstract class CustomAdapterShell[T](
    errorList: java.util.List[ErrorParser],
    clavesSAT: Map[String, String],
    validator: DataTypeCustomValidator[T]
) extends TypeAdapter[T]
     {
       private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  override def write(out: JsonWriter, value: T): Unit = ???

  override def read(in: JsonReader): T = {
    if (in.peek() == JsonToken.NULL) {
      logger.debug(s"${in.getPath} es null.")
      in.nextNull()
      null.asInstanceOf[T]
    } else {
      val tmpString = in.nextString()
      if (tmpString.equalsIgnoreCase("NULL")) {
        null.asInstanceOf[T]
      } else {
        validator.validate(tmpString) match {
          case Left(e) =>
            val claveSAT = clavesSAT(UtileriasParser.limpiarPath(in.getPath))
            errorList.add(ErrorParser(claveSAT, in.getPath, tmpString, e.getMessage))
            logger.error(s"Valor obtenido $tmpString en la ruta ${in.getPath}")
            logger.error(e)
            null.asInstanceOf[T]
          case Right(v) => v
        }
      }
    }
  }

}
