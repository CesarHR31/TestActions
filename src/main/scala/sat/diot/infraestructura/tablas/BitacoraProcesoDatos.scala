package sat.diot.infraestructura.tablas


import sat.diot.comunes.CONSTANTS
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class BitacoraProcesoDatos(
                                 idEjecucion: String,
                                 idProceso: Int,
                                 idPasoEjecucion: Int,
                                 estatus: Int,
                                 objeto: Option[String],
                                 registrosProcesados: Long,
                                 fecha_ini_ejecucion: Timestamp,
                                 fecha_fin_ejecucion: Timestamp,
                                 fecha_ini_periodo: Timestamp,
                                 fecha_fin_periodo: Timestamp,
                                 parametrosEjecucion: Option[String],
                                 detalleError: Option[String]
                               )

class BitacoraProcesoDatosTable(tag: Tag)
  extends Table[BitacoraProcesoDatos](
    tag,
    Some(CONSTANTS.DATABASE_SCHEMA_BITACORAS),
    "bitacora_proceso_datos"
  ) {

  def idEjecucion: Rep[String]                 = column[String]("id_ejecucion")
  def idProceso: Rep[Int]                      = column[Int]("id_proceso")
  def idPasoEjecucion: Rep[Int]                = column[Int]("id_paso_ejecucion")
  def estatus: Rep[Int]                        = column[Int]("estatus")
  def objeto: Rep[Option[String]]              = column[Option[String]]("objeto")
  def registrosProcesados: Rep[Long]           = column[Long]("registros_procesados")
  def fecha_ini_ejecucion: Rep[Timestamp]      = column[Timestamp]("fecha_ini_ejecucion")
  def fecha_fin_ejecucion: Rep[Timestamp]      = column[Timestamp]("fecha_fin_ejecucion")
  def fecha_ini_periodo: Rep[Timestamp]        = column[Timestamp]("fecha_ini_periodo")
  def fecha_fin_periodo: Rep[Timestamp]        = column[Timestamp]("fecha_fin_periodo")
  def parametrosEjecucion: Rep[Option[String]] = column[Option[String]]("parametros_ejecucion")
  def detalleError: Rep[Option[String]]        = column[Option[String]]("detalle_error")

  override def * : ProvenShape[BitacoraProcesoDatos] =
    (
      idEjecucion,
      idProceso,
      idPasoEjecucion,
      estatus,
      objeto,
      registrosProcesados,
      fecha_ini_ejecucion,
      fecha_fin_ejecucion,
      fecha_ini_periodo,
      fecha_fin_periodo,
      parametrosEjecucion,
      detalleError
    ) <> (BitacoraProcesoDatos.tupled, BitacoraProcesoDatos.unapply)
}

