package sat.diot.ingesta.entities


import com.microsoft.azure.storage.table.{TableEntity, TableServiceEntity}

import java.util.Date

class WATDeclaracion(

                      var partitionkey : String,
                      var rowkey : String,
                      var Obligaciones : String,
                      var Concepto: String,
                      var timestamp : Date,
                      var Rfc : String,
                      var NumeroOperacion : Long,
                      var FechaDeclaracion : Date,
                      var Ejercicio : Int,
                      var Periodicidad : String,
                      var Periodo : String,
                      var TipoDeclaracion : String,
                      var TipoComplementaria : String,
                      var EstatusDeclaracion : Int,
                      var Estatus : Int,
                      var IdentificadorDeclaracion : java.util.UUID,
                      var IdentificadorDeclaracionPadre : java.util.UUID,
                      var IdentificadorDeclaracionRaiz : java.util.UUID
                    ) extends TableServiceEntity with TableEntity with Serializable {

  this.setPartitionKey(partitionkey)
  this.setRowKey(rowkey)

  def this() {
    this(null, null, null, null, null, null, 0l, null, 0, null, null,null, null, 0, 0, null, null, null)
  }

  def setObligaciones(value: String): Unit = {
    this.Obligaciones = value
  }

  def setConcepto(value: String): Unit = {
    this.Concepto = value
  }

  def setRfc(value: String): Unit = {
    this.Rfc = value
  }


  def setNumeroOperacion(value: Long): Unit = {
    this.NumeroOperacion = value
  }

  def setFechaDeclaracion(value: Date): Unit = {
    this.FechaDeclaracion = value
  }

  def settimestamp(value: Date): Unit = {
    this.timestamp = value
  }

  def setEjercicio(value: Int): Unit = {
    this.Ejercicio = value
  }

  def setPeriodicidad(value: String): Unit = {
    this.Periodicidad = value
  }

  def setPeriodo(value: String): Unit = {
    this.Periodo = value
  }

  def setTipoDeclaracion(value: String): Unit = {
    this.TipoDeclaracion = value
  }

  def setTipoComplementaria(value: String): Unit = {
    this.TipoComplementaria = value
  }

  def setEstatusDeclaracion(value: Int): Unit = {
    this.EstatusDeclaracion = value
  }

  def setEstatus(value: Int): Unit = {
    this.Estatus = value
  }

  def setIdentificadorDeclaracion(value: java.util.UUID): Unit = {
    this.IdentificadorDeclaracion = value
  }

  def setIdentificadorDeclaracionPadre(value: java.util.UUID): Unit = {
    this.IdentificadorDeclaracionPadre = value
  }

  def setIdentificadorDeclaracionRaiz(value: java.util.UUID): Unit = {
    this.IdentificadorDeclaracionRaiz = value
  }


  // Get methods
  def getRfc: String = {
    this.Rfc
  }
  def getObligaciones: String = {
    this.Obligaciones
  }

  def getConcepto: String = {
    this.Concepto
  }

  def getNumeroOperacion: Long = {
    this.NumeroOperacion
  }

  def getFechaDeclaracion: Date = {
    this.FechaDeclaracion
  }

  def gettimestamp: Date = {
    this.timestamp
  }

  def getEjercicio: Int = {
    this.Ejercicio
  }

  def getPeriodicidad: String = {
    this.Periodicidad
  }

  def getPeriodo: String = {
    this.Periodo
  }

  def getTipoDeclaracion: String = {
    this.TipoDeclaracion
  }

  def getTipoComplementaria: String = {
    this.TipoComplementaria
  }

  def getEstatusDeclaracion: Int = {
    this.EstatusDeclaracion
  }

  def getEstatus: Int = {
    this.Estatus
  }

  def getIdentificadorDeclaracion: java.util.UUID = {
    this.IdentificadorDeclaracion
  }

  def getIdentificadorDeclaracionPadre: java.util.UUID = {
    this.IdentificadorDeclaracionPadre
  }

  def getIdentificadorDeclaracionRaiz: java.util.UUID = {
    this.IdentificadorDeclaracionRaiz
  }
}
