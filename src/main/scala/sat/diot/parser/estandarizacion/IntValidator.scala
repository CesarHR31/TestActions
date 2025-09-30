package sat.diot.parser.estandarizacion

object IntValidator extends DataTypeCustomValidator[Integer] {
  override def stringToType(value: String): Integer = value.toInt
}
