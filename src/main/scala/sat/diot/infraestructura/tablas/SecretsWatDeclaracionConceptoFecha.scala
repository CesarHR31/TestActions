package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class SecretsWatDeclaracionConceptoFechaTable(
                                                    wat: String,
                                                    secret: String,
                                                    scope: String,
                                                    vigente: Boolean,
                                                    fechaactualizacion: Timestamp,
                                                    fecharegistro: Timestamp
                                                  )

class SecretsWatDeclaracionConceptoFechaTables(tag: Tag) extends Table[SecretsWatDeclaracionConceptoFechaTable](tag, Some("diot_ctl"), "secrets_wat_declaracionconceptofecha") {
  override def * : ProvenShape[SecretsWatDeclaracionConceptoFechaTable] =
    (
      wat,
      secret,
      scope,
      vigente,
      fechaactualizacion,
      fecharegistro,
    ) <> (SecretsWatDeclaracionConceptoFechaTable.tupled, SecretsWatDeclaracionConceptoFechaTable.unapply)

  def wat = column[String]("wat")

  def secret = column[String]("secret")

  def scope = column[String]("scope")

  def vigente = column[Boolean]("vigente")

  def fechaactualizacion = column[Timestamp]("fechaactualizacion")

  def fecharegistro = column[Timestamp]("fecharegistro")
}
