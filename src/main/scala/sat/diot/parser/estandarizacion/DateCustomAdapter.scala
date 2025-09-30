package sat.diot.parser.estandarizacion

import sat.diot.parser.ErrorParser

import java.sql.Date

class DateCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
  extends CustomAdapterShell[Date](errorList, clavesSAT, DateValidator) {}
