package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class GzipControlTable(
    id_ejecucion: String,
    nombretabla: String,
    id_tabla_npsi: Int,
    rutagzip: String,
    estatus: Int,
    fechaactualizacion: Timestamp,
    fecharegistro: Timestamp
)

class GzipControlTables(tag: Tag) extends Table[GzipControlTable](tag, Some("diot_ctl"), "control_gzip") {
  override def * : ProvenShape[GzipControlTable] =
    (
      id_ejecucion,
      nombretabla,
      id_tabla_npsi,
      rutagzip,
      estatus,
      fechaactualizacion,
      fecharegistro
    ) <> (GzipControlTable.tupled, GzipControlTable.unapply)

  def id_ejecucion = column[String]("id_ejecucion")

  def nombretabla = column[String]("nombretabla")

  def id_tabla_npsi = column[Int]("id_tabla_npsi")

  def rutagzip = column[String]("rutagzip")

  def estatus = column[Int]("estatus")

  def fechaactualizacion = column[Timestamp]("fechaactualizacion")

  def fecharegistro = column[Timestamp]("fecharegistro")
}
