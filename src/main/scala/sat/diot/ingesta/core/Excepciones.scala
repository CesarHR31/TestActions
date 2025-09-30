package sat.diot.ingesta.core

object Excepciones {

  final case class LandException(
      private val message: String = "Ocurrio un error al crear Land",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)

  final case class LandDFException(
      private val message: String = "Ocurrio un error al crear el Dataframe del Land",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)

  final case class GZIPException(
      private val message: String = "Ocurrio un error al crear el archivo GZIP",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)

  final case class PostgresWriteException(
      private val message: String = "Ocurrio un Error al intentar escribir hacia PostgreSQL",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)

  final case class PostgresReadException(
      private val message: String = "Ocurrio un Error al intentar leer desde PostgreSQL",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)

}
