package sat.diot.conciliacion

import org.apache.spark.sql.DataFrame
import sat.diot.conciliacion.TipoConciliacion.TipoConciliacion

import java.sql.Timestamp

case class ResultadoConteo(conteo: Long, insumos: DataFrame)

case class ResultadoConciliaciones(
                                    tipoConciliacion: String,
                                    nombreTtabla: String,
                                    diferencia: Long,
                                    either: Either[Throwable, (Long, Long)]
                                  )

case class RegistroBitacora(
                             fechainicial: Timestamp,
                             fechafinal: Timestamp,
                             modeloaconciliar: String,
                             tablaaconciliar: String,
                             duplicados: Long,
                             conteoinicialesperado: Long,
                             conteoinicialactual: Long,
                             diferenciainicial: Long,
                             conteofinalesperado: Option[Long],
                             conteofinalactual: Option[Long],
                             diferenciafinal: Option[Long],
                             procesorealizado: String,
                             exitoso: Boolean,
                             error: String,
                             tiempoenminutostardoenprocesar: Long,
                             timestampinsercionregistro: Timestamp
                           )

case class RegistroConciliacion(fechainicial: String,
                                fechafinal: String,
                                modeloaconciliar: String,
                                tablaaconciliar: String,
                                duplicados: Long,
                                conteoinicialesperado: Long,
                                conteoinicialactual: Long,
                                diferenciainicial: Long,
                                conteofinalesperado: Option[Long],
                                conteofinalactual: Option[Long],
                                diferenciafinal: Option[Long],
                                procesorealizado: String,
                                exitoso: Boolean,
                                error: String,
                                tiempoenminutostardoenprocesar: Long
                               )

case class EntidadProcesoConciliacion(fechaInicial: String,
                                      fechaFinal: String,
                                      nombreTablaBase: String,
                                      nombreTablaAConciliar: String,
                                      tipoConciliacion: TipoConciliacion,
                                      idEjecucion: String)

