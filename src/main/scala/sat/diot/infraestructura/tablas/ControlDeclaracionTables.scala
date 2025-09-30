package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class ControlDeclaracionTable(
    id: Long,
    id_ejecucion: String,
    blobpath: String,
    rfc: String,
    numeroOperacion: Long,
    obligacion: String,
    fechaPresentacion: Timestamp,
    ejercicio: Int,
    fechaProcesado: Timestamp,
    flagReenvio: Boolean,
    error: Boolean
)

class ControlDeclaracionTables(tag: Tag) extends Table[ControlDeclaracionTable](tag, Some("diot_ctl"), "control_declaracion") {

  override def * : ProvenShape[ControlDeclaracionTable] =
    (
      id,
      id_ejecucion,
      blobpath,
      rfc,
      numeroOperacion,
      obligacion,
      fechaPresentacion,
      ejercicio,
      fechaProcesado,
      flagReenvio,
      error
    ) <> (ControlDeclaracionTable.tupled, ControlDeclaracionTable.unapply)

  def id: Rep[Long]            = column[Long]("id", O.PrimaryKey)

  def id_ejecucion: Rep[String] = column[String]("id_ejecucion")

  def blobpath: Rep[String]    = column[String]("blobpath")

  def rfc                      = column[String]("rfc")

  def numeroOperacion          = column[Long]("numerooperacion")

  def obligacion               = column[String]("obligacion")

  def fechaPresentacion        = column[Timestamp]("fechapresentacion")

  def ejercicio                = column[Int]("ejercicio")

  def fechaProcesado           = column[Timestamp]("fechaprocesado")

  def flagReenvio              = column[Boolean]("flagreenvio")

  def error                    = column[Boolean]("error")

}
