package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class MonitoreoGeneralNPSIControlTable(
    id_ejecucion: String,
    id_proceso: Int,
    id_paso_ejecucion: Int,
    id_tabla: Int,
    registros_origen: Long,
    registros_proceso: Long,
    exitoso: Boolean,
    tiempo_de_execucion: Int,
    detalle_error: String,
    fecha_ejecucion_proceso: Timestamp
)

class MonitoreoGeneralNPSIControlTables(tag: Tag)
    extends Table[MonitoreoGeneralNPSIControlTable](tag, Some("bitacoras"), "monitoreo_general") {
  override def * : ProvenShape[MonitoreoGeneralNPSIControlTable] =
    (
      id_ejecucion,
      id_proceso,
      id_paso_ejecucion,
      id_tabla,
      registros_origen,
      registros_proceso,
      exitoso,
      tiempo_de_execucion,
      detalle_error,
      fecha_ejecucion_proceso
    ) <> (MonitoreoGeneralNPSIControlTable.tupled, MonitoreoGeneralNPSIControlTable.unapply)

  def id_ejecucion = column[String]("id_ejecucion")

  def id_proceso = column[Int]("id_proceso")

  def id_paso_ejecucion = column[Int]("id_paso_ejecucion")

  def id_tabla = column[Int]("id_tabla")

  def registros_origen = column[Long]("registros_origen")

  def registros_proceso = column[Long]("registros_proceso")

  def exitoso = column[Boolean]("exitoso")

  def tiempo_de_execucion = column[Int]("tiempo_de_execucion")

  def detalle_error = column[String]("detalle_error")

  def fecha_ejecucion_proceso = column[Timestamp]("fecha_ejecucion_proceso")
}
