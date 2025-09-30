package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class HistoricoTable(
    id_ejecucion: String,
    fechaprocesamiento: Timestamp
)

class HistoricoTables(tag: Tag) extends Table[HistoricoTable](tag, Some("diot_ctl"), "idejecucion_historico") {

  override def * : ProvenShape[HistoricoTable] =
    (
      id_ejecucion,
      fechaProcesamiento
    ) <> (HistoricoTable.tupled, HistoricoTable.unapply)

  def id_ejecucion: Rep[String]           = column[String]("id_ejecucion", O.PrimaryKey)

  def fechaProcesamiento: Rep[Timestamp] = column[Timestamp]("fechaprocesamiento")

}
