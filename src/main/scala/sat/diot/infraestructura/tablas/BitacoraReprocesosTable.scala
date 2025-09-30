package sat.diot.infraestructura.tablas

import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape
import java.sql.Timestamp

case class BitacoraReproceso(
                              idejecucion: String,
                              fechainicio: Timestamp,
                              fechafin: Timestamp,
                              estatus: Int,
                              id_proceso: Int,
                              fechainsercion: Timestamp,
                              fechaactualizacion: Timestamp
                            )

class BitacoraReprocesosTable (tag: Tag) extends Table[BitacoraReproceso](tag, Some("bitacoras_dyp"), "bitacorareprocesos") {
  override def * : ProvenShape[BitacoraReproceso] =
    (
      idejecucion,
      fechainicio,
      fechafin,
      estatus,
      id_proceso,
      fechainsercion,
      fechaactualizacion,
    ) <> (BitacoraReproceso.tupled, BitacoraReproceso.unapply)

  def idejecucion = column[String]("idejecucion")

  def fechainicio = column[Timestamp]("fechainicio")

  def fechafin = column[Timestamp]("fechafin")

  def estatus = column[Int]("estatus")

  def id_proceso = column[Int]("id_proceso")

  def fechainsercion = column[Timestamp]("fechainsercion")

  def fechaactualizacion = column[Timestamp]("fechaactualizacion")
}

