package sat.diot.comunes

import sat.diot.comunes.config.ConfigurationProvider

object CONSTANTS {

  val DATABASE_SCHEMA_BITACORAS = "bitacoras_dyp"
  val BITACORAS_ID_PROCESO: Int = ConfigurationProvider.identificadorIdProceso
  val BITACORAS_PASO_RECEPCION = 159
  val BITACORAS_PASO_DESDOBLE = 160
  val BITACORAS_PASO_PLATA = 161
  val BITACORAS_PASO_ORO = 162
}
