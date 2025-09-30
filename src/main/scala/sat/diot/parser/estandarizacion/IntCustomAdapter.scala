package sat.diot.parser.estandarizacion

import sat.diot.parser.ErrorParser

class IntCustomAdapter(errorList: java.util.List[ErrorParser], clavesSAT: Map[String, String])
    extends CustomAdapterShell[Integer](errorList, clavesSAT, IntValidator) {}
