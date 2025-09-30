package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

case class CatalogoTable(
    idEstatus: Int,
    descripcionEstatus: String
)

class CatalogoTables(tag: Tag) extends Table[CatalogoTable](tag, Some("diot_ctl"), "estatus_catalogo") {

  override def * : ProvenShape[CatalogoTable] =
    (
      idEstatus,
      descripcionEstatus
    ) <> (CatalogoTable.tupled, CatalogoTable.unapply)

  def idEstatus: Rep[Int]             = column[Int]("idestatus", O.PrimaryKey)

  def descripcionEstatus: Rep[String] = column[String]("descripcionestatus")

}
