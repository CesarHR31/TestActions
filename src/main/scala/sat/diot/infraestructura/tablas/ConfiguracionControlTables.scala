package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

case class ConfiguracionControlTable(
    nombredocumento: String,
    identificador: String,
    basedatoscontrol: String,
    fechainicio: String
)

class ConfiguracionControlTables(tag: Tag) extends Table[ConfiguracionControlTable](tag, Some("diot_ctl"), "configuracion") {

  override def * : ProvenShape[ConfiguracionControlTable] =
    (
      nombredocumento,
      identificador,
      basedatoscontrol,
      fechainicio
    ) <> (ConfiguracionControlTable.tupled, ConfiguracionControlTable.unapply)

  def nombredocumento: Rep[String] = column[String]("nombredocumento", O.PrimaryKey)

  def identificador: Rep[String]   = column[String]("identificador")

  def basedatoscontrol             = column[String]("basedatoscontrol")

  def fechainicio                  = column[String]("fechainicio")

}
