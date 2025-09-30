package sat.diot.ingesta.entities

import com.microsoft.azure.storage.table.{TableEntity, TableServiceEntity}

import java.util.Date

class WATDescarga(
                   var FechaCarga: String,
                   var IdentificadorDeclaracion: String,
                   var Rfc: String,
                   var NumeroOperacion: Long,
                   var Obligaciones: String,
                   var FechaDeclaracion: Date,
                   var Ejercicio: Int
                 ) extends TableServiceEntity
  with TableEntity
  with Serializable {

  this.setPartitionKey(FechaCarga)
  this.setRowKey(IdentificadorDeclaracion)

  def this() {
    this(null, null, null, 0L, null, null, 0)
  }

  def getFechaCarga: String = {
    this.FechaCarga
  }

  def setFechaCarga(value: String): Unit = {
    this.FechaCarga = value
  }

  def getFechaDeclaracion: Date = {
    this.FechaDeclaracion
  }

  def setFechaDeclaracion(value: Date): Unit = {
    this.FechaDeclaracion = value
  }

  def getRfc: String = {
    this.Rfc
  }

  def setRfc(value: String): Unit = {
    this.Rfc = value
  }

  def getObligaciones: String = {
    this.Obligaciones
  }

  def setObligaciones(value: String): Unit = {
    this.Obligaciones = value
  }

  def getNumeroOperacion: Long = {
    this.NumeroOperacion
  }

  def setNumeroOperacion(value: Long): Unit = {
    this.NumeroOperacion = value
  }

  def getEjercicio: Int = {
    this.Ejercicio
  }

  def setEjercicio(value: Int): Unit = {
    this.Ejercicio = value
  }
}
