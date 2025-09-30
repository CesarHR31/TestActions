package sat.diot.parser.estandarizacion

import sat.diot.parser.ErrorParser

import java.math.{BigDecimal => JBigDecimal}

object DecimalValidator extends DataTypeCustomValidator[JBigDecimal] {
  override def stringToType(value: String): JBigDecimal = new JBigDecimal(value)
}

class DecimalCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
  extends CustomAdapterShell[JBigDecimal](errorList, clavesSAT, DecimalValidator) {}

object ShortValidator extends DataTypeCustomValidator[java.lang.Short] {
  override def stringToType(value: String): java.lang.Short = value.toShort
}

class ShortCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
  extends CustomAdapterShell[java.lang.Short](errorList, clavesSAT, ShortValidator) {}


object LongValidator extends DataTypeCustomValidator[java.lang.Long] {
  override def stringToType(value: String): java.lang.Long = value.toLong
}

class LongCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
  extends CustomAdapterShell[java.lang.Long](errorList, clavesSAT, LongValidator) {}
