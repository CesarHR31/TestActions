package sat.diot.comunes

import sat.diot.comunes.config.ConfigurationProvider

object CatalogoProcesoNPSI {
  val ProcesoDesdobleInformacion = ConfigurationProvider.identificadorIdProceso
}

object CatalogoPasoEjecucionNPSI {
  val IntegracionLand = 15
  val IntegracionBronce = 16
  val IntegracionPlata = 17
  val ConciliacioncifrascontrolPlata = 18
  val IntegracionOro = 19
  val ConciliacioncifrascontrolOro = 20
  val GeneracionGZ = 21
  val EnvioGZ = 22
  val EnvioCitus = 4
  val SincronizacionListadoRecepcion = 166
  val GeneracionSolicitudFaltantes = 167
  val EliminacionFaltantesCuarentena = 168
  val BuscarFaltantesEnDesdoble = 169
  val ConfiguracionReprocesos = 170
}

object CatalogoEstatusProcesoDatos{
  val Exito = 1
  val Error = 2
  val Alerta = 3
}