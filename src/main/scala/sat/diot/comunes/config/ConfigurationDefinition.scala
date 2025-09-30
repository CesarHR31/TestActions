package sat.diot.comunes.config

import java.util

final case class ConfigurationDefinition(
                                          postgresqlControlUrl: DatabricksSecret,
                                          controlServidor: DatabricksSecret,
                                          controlPuerto: DatabricksSecret,
                                          controlBd: DatabricksSecret,
                                          controlUsuario: DatabricksSecret,
                                          controlPass: DatabricksSecret,
                                          storageControlConnString: DatabricksSecret,
                                          storageControlConnStringReprocesos: DatabricksSecret,
                                          storageControlConnStringUAT: DatabricksSecret,
                                          citusServidor: DatabricksSecret,
                                          citusPuerto: DatabricksSecret,
                                          citusUsuario: DatabricksSecret,
                                          citusPass: DatabricksSecret,
                                          citusBd: DatabricksSecret,
                                          sasBlobStorage: DatabricksSecret,
                                          URICuentaStorageArchivosJSON: DatabricksSecret,
                                          identificadorWATConsulta: String,
                                          identificadorBaseBronce: String,
                                          identificadorBaseLand: String,
                                          identificadorBaseControl: String,
                                          identificadorBasePlata: String,
                                          identificadorBaseOro: String,

                                          //-------------------------------------
                                          applicationInsightsKey: DatabricksSecret,
                                          proyecto: String,
                                          mountCSVNPSI: String,
                                          prefijoCsvDeltasOro: String,
                                          mapeoTablasDB2: util.HashMap[String, String],
                                          identificadorCapa: String,
                                          identificadorAplicacion: String,
                                          identificadorAmbiente: String,
                                          identificadorCodigoAplicacion: String,
                                          identificadorOrigen: String,
                                          identificadorPorcentajeCuarentenaPermitido: Int,
                                          identificadorFechaUltimaVigenciaOro: String,
                                          identificadorEsquemaCitus: String,
                                          identificadorTablaUltimaDeclaracionVigente: String,
                                          identificadorIdTablaUltimaDeclaracionVigenteCitus: Int,
                                          identificadorTablaPartitionsKeysRfc: String,
                                          identificadorTablaPartitionsKeysRfcObligaciones: String,
                                          identificadorIdProceso: Int,
                                          idEstadoProceso: Int,
                                          ejercicioDeclaracion: String,
                                          identificadorBaseConciliacion: String,
                                          identificadorBaseStaging: String,
                                          identificadorPathCsvFaltantes: String,
                                          identificadorEsquemaBitacorasSingle: String,
                                          identificadorTablaContadorReporte: String,
                                          identificadorTablaContadorPSQLPostgreSQL: String,
                                          identificadorTablaBitacoraReprocesosSingle: String,
                                          identificadorReintentosReprocesamiento: Int,
                                          databases: Array[SparkDatabase],
                                          limiteArchivosNormales: Long,
                                          limiteArchivosGrandes: Long,
                                          identificadorTablaOroTemporal: String,
                                          identificadorTablaTemporalFlujo: String,
                                          obligaciones: String,
                                          agrupacionSurferJson: Integer,
                                          idTipoProcesoDatos: String
                                        )

final case class DatabricksSecret(scope: String, key: String)

final case class SparkDatabase(name: String, tables: Array[SparkTable])

final case class SparkTable(name: String, id: String, idTabla: Int)

case class TablaOro(nombre: String, id: String, idTabla: Int)

object TablaOro {
  def unapply(tabla: TablaOro): Option[(String, String, Int)] = {
    Some((tabla.nombre, tabla.id, tabla.idTabla))
  }
}