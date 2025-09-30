package sat.diot.parser.estandarizacion

import java.sql.Timestamp
import scala.util.Try

object TimestampValidator extends DataTypeCustomValidator[Timestamp] {
  override def stringToType(value: String): Timestamp = convierteStringDeFecha(value)

  private def convierteStringDeFecha(fechaEnString: String): Timestamp = {
    def tryParseString(fecha: String, formato: String): Timestamp = {
      val dateFormat = new java.text.SimpleDateFormat(formato)
      dateFormat.setLenient(false)
      val parsedDate = dateFormat.parse(fecha)
      new Timestamp(parsedDate.getTime)
    }

    val availableFormats = Seq("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss")

    val results = availableFormats
      .map(v => Try(tryParseString(fechaEnString, v)))
      .find(v => v.isSuccess)

    results match {
      case Some(value) => value.get
      case None =>
        throw new IllegalArgumentException(
          s"Value `$fechaEnString` could not be parsed with available formats: (${availableFormats.mkString(",")})"
        )
    }
  }

}
