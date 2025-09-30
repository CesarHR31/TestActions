package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._

import java.sql.Timestamp

case class PostgresAzureAccountTable(
    nombreDocumento: String,
    identificador: String,
    cuenta: String,
    contenedor: String,
    endpointConSas: String,
    vigencia: String,
    fechaActualizacion: Timestamp
)

class PostgresAzureAccountTables(tag: Tag) extends Table[PostgresAzureAccountTable](tag, Some("diot_ctl"), "tabla_sas") {

  override def * =
    (
      nombredocumento,
      identificador,
      cuenta,
      contenedor,
      endpointConSas,
      vigencia,
      fechaActualizacion
    ) <> (PostgresAzureAccountTable.tupled, PostgresAzureAccountTable.unapply)

  def nombredocumento: Rep[String]       = column[String]("nombre_documento", O.PrimaryKey)

  def identificador: Rep[String]         = column[String]("identificador")

  def cuenta: Rep[String]                = column[String]("cuenta")

  def contenedor: Rep[String]            = column[String]("contenedor")

  def endpointConSas: Rep[String]        = column[String]("endpoint_con_sas")

  def vigencia: Rep[String]              = column[String]("vigencia")

  def fechaActualizacion: Rep[Timestamp] = column[Timestamp]("fecha_actualizacion")
}
