package sat.diot.infraestructura.tablas


import sat.diot.comunes.config.ConfigurationProvider
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class ConfiguracionWATDeclaracionAnual(
                                             nombrewatconcepto: String,
                                             nombrewatconceptodetalle: String,
                                             secret: String,
                                             scope: String,
                                             vigente: Boolean,
                                             fechaactualizacion: Timestamp,
                                             fecharegistro: Timestamp
                                           )

class ConfiguracionWATDeclaracionAnualTable (tag: Tag) extends Table[ConfiguracionWATDeclaracionAnual](tag, Some(ConfigurationProvider.identificadorBaseControl), "config_declaracion_anual_wat") {
  override def * : ProvenShape[ConfiguracionWATDeclaracionAnual] =
    (
      nombrewatconcepto,
      nombrewatconceptodetalle,
      secret,
      scope,
      vigente,
      fechaactualizacion,
      fecharegistro,
    ) <> (ConfiguracionWATDeclaracionAnual.tupled, ConfiguracionWATDeclaracionAnual.unapply)

  def nombrewatconcepto = column[String]("nombrewatconcepto")
  def nombrewatconceptodetalle= column[String]("nombrewatconceptodetalle")

  def secret = column[String]("secret")

  def scope = column[String]("scope")

  def vigente = column[Boolean]("vigente")

  def fechaactualizacion = column[Timestamp]("fechaactualizacion")

  def fecharegistro = column[Timestamp]("fecharegistro")
}
