package sat.diot.parser.estandarizacion

import java.sql.Date

object DateValidator extends DataTypeCustomValidator[Date] {
  override def stringToType(value: String): Date = Date.valueOf(value)
}
