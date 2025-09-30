package sat.diot.infraestructura.tablas

import sat.diot.comunes.Util
import sat.diot.ingesta.entities.CatalogoProceso
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape

import java.sql.Timestamp

case class CifrasControlTable(
    id_ejecucion: String,
    fechaInicio: Timestamp,
    fechaFin: Timestamp,
    cifrasRecepcion: Long,
    cifrasBronce: Long,
    cifrasBronceValidos: Long,
    cifrasBronceError: Long,
    cifrasPlata: Long,
    cifrasOro: Long,
    fechaInsercion: Timestamp,
    idEstatus: Int,
    fechaActualizacion: Timestamp,
    nombreWat: String,
    esReproceso: Boolean,
    secretScope: String,
    secretKey: String,
    urlStorage: String
) {
  def this(
      idproc: String,
      fi: Timestamp,
      ff: Timestamp,
      nombreWat: String,
      esReproceso: Boolean,
      secretScope: String,
      secretKey: String,
      urlStorage: String
  ) =
    this(
      idproc,
      fi,
      ff,
      -1L,
      -1L,
      -1L,
      -1L,
      -1L,
      -1L,
      Util.obtenerTimestamp(),
      CatalogoProceso.REGISTRADO,
      Util.obtenerTimestamp(),
      nombreWat,
      esReproceso,
      secretScope,
      secretKey,
      urlStorage
    )
}

class CifrasControlTables(tag: Tag) extends Table[CifrasControlTable](tag, Some("diot_ctl"), "cifras_control") {

  override def * : ProvenShape[CifrasControlTable] =
    (
      id_ejecucion,
      fechaInicio,
      fechaFin,
      cifrasRecepcion,
      cifrasBronce,
      cifrasBronceValidos,
      cifrasBronceError,
      cifrasPlata,
      cifrasOro,
      fechaInsercion,
      idEstatus,
      fechaActualizacion,
      nombreWat,
      esReproceso,
      secretScope,
      secretKey,
      urlStorage
    ) <> (CifrasControlTable.tupled, CifrasControlTable.unapply)

  def id_ejecucion: Rep[String]    = column[String]("id_ejecucion", O.PrimaryKey)

  def fechaInicio: Rep[Timestamp] = column[Timestamp]("fechainicio")

  def fechaFin: Rep[Timestamp]    = column[Timestamp]("fechafin")

  def cifrasRecepcion: Rep[Long]  = column[Long]("cifrasrecepcion")

  def cifrasBronce: Rep[Long]     = column[Long]("cifrasbronce")

  def cifrasBronceValidos         = column[Long]("cifrasbroncevalidos")

  def cifrasBronceError           = column[Long]("cifrasbronceerror")

  def cifrasPlata                 = column[Long]("cifrasplata")

  def cifrasOro                   = column[Long]("cifrasoro")

  def fechaInsercion              = column[Timestamp]("fechainsercion")

  def idEstatus                   = column[Int]("idestatus")

  def fechaActualizacion          = column[Timestamp]("fechaactualizacion")

  def nombreWat: Rep[String]      = column[String]("nombrewat")

  def esReproceso: Rep[Boolean]   = column[Boolean]("esreproceso")

  def secretScope: Rep[String]    = column[String]("secret_scope")

  def secretKey: Rep[String]      = column[String]("secret_key")

  def urlStorage: Rep[String]     = column[String]("storage_url")

}
