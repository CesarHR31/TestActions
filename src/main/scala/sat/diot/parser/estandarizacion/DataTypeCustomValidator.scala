package sat.diot.parser.estandarizacion

import sat.diot.comunes.config.ConfigurationProvider

import scala.util.control.NonFatal
import scala.reflect.runtime.universe.{TypeTag, typeOf}

abstract class DataTypeCustomValidator[T: TypeTag]  {
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  def validate(value: String): Either[Throwable, T] = {
    try {
      Right(stringToType(value))
    } catch {
      case NonFatal(e) =>
        logger.error(s"Cannot parse $value. to ${typeOf[T]}")
        Left(e)
    }
  }

  def stringToType(value: String): T
}
