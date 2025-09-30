package sat.diot.parser.estandarizacion

import sat.diot.parser.ErrorParser

object BooleanValidator extends DataTypeCustomValidator[Boolean] {
  override def stringToType(value: String): Boolean = value.toBoolean
}

class BooleanCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
    extends CustomAdapterShell[Boolean](errorList, clavesSAT, BooleanValidator) {}
