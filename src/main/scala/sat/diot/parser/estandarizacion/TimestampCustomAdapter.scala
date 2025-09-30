package sat.diot.parser.estandarizacion

import sat.diot.parser.ErrorParser

import java.sql.Timestamp

class TimestampCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
  extends CustomAdapterShell[Timestamp](errorList, clavesSAT, TimestampValidator) {}