package sat.diot.infraestructura.tablas

import sat.diot.comunes.CONSTANTS

import java.sql.Timestamp
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

case class BitacoraDetallePaso(
                                idEjecucion: String,
                                idProceso: Int,
                                idPasoEjecucion: Int,
                                detallePaso: String,
                                objeto: String,
                                registrosProcesados: Int,
                                fechaEjecucion: Timestamp
                              )

class BitacoraDetallePasoTable(tag: Tag)
  extends Table[BitacoraDetallePaso](
    tag,
    Some(CONSTANTS.DATABASE_SCHEMA_BITACORAS),
    "bitacora_detalle_paso"
  ) {

  def idEjecucion: Rep[String]       = column[String]("id_ejecucion")
  def idProceso: Rep[Int]            = column[Int]("id_proceso")
  def idPasoEjecucion: Rep[Int]      = column[Int]("id_paso_ejecucion")
  def detallePaso: Rep[String]       = column[String]("detalle_paso")
  def objeto: Rep[String]            = column[String]("objeto")
  def registrosProcesados: Rep[Int]  = column[Int]("registros_procesados")
  def fechaEjecucion: Rep[Timestamp] = column[Timestamp]("fecha_ejecucion")

  override def * : ProvenShape[BitacoraDetallePaso] = (
    idEjecucion,
    idProceso,
    idPasoEjecucion,
    detallePaso,
    objeto,
    registrosProcesados,
    fechaEjecucion
  ) <> (BitacoraDetallePaso.tupled, BitacoraDetallePaso.unapply)
}

