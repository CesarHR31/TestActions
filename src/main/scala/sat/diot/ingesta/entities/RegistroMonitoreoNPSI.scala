package sat.diot.ingesta.entities

case class RegistroMonitoreoNPSI(idEjecucion: String,
                                 idPasoEjecucion: Int,
                                 idTabla: Int,
                                 nombreTabla: String,
                                 registrosOrigen: Long,
                                 registrosProceso: Long,
                                 exitoso: Boolean,
                                 tiempoDeEjecucion: Int,
                                 detalleError: String,
                                 esCuarentena: Boolean = false)
