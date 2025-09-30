package sat.diot.conciliacion

object TipoConciliacion extends Enumeration {
  type TipoConciliacion = Value
  val oro, plata = Value
}

object ProcesoConciliacion extends Enumeration {
  type ProcesoConciliacion = Value
  val FALLO_CATASTROFICO, CONTEOS_IGUALES, CONCILIACION_EJECUTADA = Value
}
