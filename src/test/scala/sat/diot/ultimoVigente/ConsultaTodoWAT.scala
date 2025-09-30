package sat.diot.ultimoVigente

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.{CloudTable, CloudTableClient}
import org.apache.spark.sql.functions.{col, date_trunc, to_date}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.SparkSessionManager
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.ingesta.core.ultimaDeclaracionVigenteLand.{DateFormat, spark, tablaTemporalLandConcepto, tablaTemporalLandConceptoDetalle}
import sat.diot.ingesta.entities.EsquemasCapas.UltimaVigenteLand
import sat.diot.ingesta.entities.WATDeclaracion
import sat.diot.ingesta.utils.AzureTable

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

class ConsultaTodoWAT extends AnyFunSuite {

  SparkSessionManager.localSpark = true
  lazy val spark: SparkSession = SparkSessionManager.session

  import spark.implicits._

  val rutaUltimaVigente = "E:\\temp\\diot\\ultimavigente"
  val rutaUltimaVigenteDet = "E:\\temp\\diot\\ultimavigenteDetalle"

  /*Test para obtener datos de las WATS DeclaracionConcepto y DeclaracionConceptoDetalle 2025*/
  test("Consulta toda la WAT DeclaracionConcepto") {
    val connString = "SharedAccessSignature=sv=2022-11-02&ss=t&srt=sco&sp=rwdlacu&se=2030-01-09T05:46:55Z&st=2025-01-08T21:46:55Z&spr=https&sig=Scrji5lE6vNHbBmsXYWzCFDos%2Fn8W2jelrEXo8YBPgk%3D;TableEndpoint=https://eu2ideuatstadeclara2025.table.core.windows.net/;"
    val watNameConcepto = "DeclaracionConcepto"
    val watNameConceptoDetalle = "DeclaracionConceptoDetalle"

    val DateFormat = "yyyy-MM-dd HH:mm:ss"
    val azure = new AzureTable(connString, watNameConcepto)
    val fi = "2025-01-06 00:00:00"
    val ff = "2025-01-19 23:59:59"
    val complementoConsulta = s""" Ejercicio ge ${ConfigurationProvider.ejercicioDeclaracion.toString} and IdEstadoProceso eq ${ConfigurationProvider.idEstadoProceso.toString} and Timestamp ge datetime'${fi.replace(" ", "T")}' and Timestamp le datetime'${ff.replace(" ", "T")}'"""
    val obligaciones = ConfigurationProvider.obtenerObligaciones
    val idEjecucion = "test"


    val result = azure.queryDeclaracionAnual(complementoConsulta)
      .asScala
      .toList
      .map(a => {
        val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
        val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
        val time = new SimpleDateFormat(s"$DateFormat")
        time.setTimeZone(TimeZone.getTimeZone("UTC"))
        val timeStamp = time.format(a.getTimestamp)
        UltimaVigenteLand(
          a.getPartitionKey,
          a.getRowKey,
          Timestamp.valueOf(timeStamp),
          a.getObligaciones,
          null,
          a.getRfc,
          a.getNumeroOperacion,
          Timestamp.valueOf(fechaDeclaracionFormato),
          a.getEjercicio,
          a.getPeriodicidad,
          a.getPeriodo,
          a.getTipoDeclaracion,
          a.getTipoComplementaria,
          a.getEstatusDeclaracion,
          a.getIdentificadorDeclaracion.toString,
          a.getIdentificadorDeclaracionPadre.toString,
          a.getIdentificadorDeclaracionRaiz.toString,
          idEjecucion
        )
      })
      .toDF()
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))

    result
      .filter($"obligaciones".contains(obligaciones))
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(rutaUltimaVigente)

    val azureDetalle = new AzureTable(connString, watNameConceptoDetalle)
    val complementoConsultaDetalle = s"""Timestamp ge datetime'${fi.replace(" ", "T")}' and Timestamp le datetime'${ff.replace(" ", "T")}'"""
    val resultDetalle = azureDetalle.queryDeclaracionAnual(complementoConsultaDetalle)
      .asScala
      .toList
      .map(a => {
        val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
        val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
        val time = new SimpleDateFormat(s"$DateFormat")
        time.setTimeZone(TimeZone.getTimeZone("UTC"))
        val timeStamp = time.format(a.getTimestamp)
        UltimaVigenteLand(
          a.getPartitionKey,
          a.getRowKey,
          Timestamp.valueOf(timeStamp),
          null,
          a.getConcepto,
          a.getPartitionKey.split("\\.")(0),
          a.getNumeroOperacion,
          Timestamp.valueOf(fechaDeclaracionFormato),
          a.getEjercicio,
          a.getPeriodicidad,
          a.getPeriodo,
          a.getTipoDeclaracion,
          a.getTipoComplementaria,
          a.getEstatus,
          a.getIdentificadorDeclaracion.toString,
          null, //a.getIdentificadorDeclaracionPadre.toString,
          null, //a.getIdentificadorDeclaracionRaiz.toString,
          idEjecucion
        )
      })
      .toDF()
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))

    resultDetalle
      .filter($"concepto".contains(obligaciones))
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(rutaUltimaVigenteDet)

    println("Se finalizó con la consulta a la WAT DeclaracionConcepto")

  }
  test("UltimaVigenteDetalle"){
    spark.read.format("delta").load(rutaUltimaVigenteDet).show(50, false)
  }
  test("genera ultima vigente") {
    val ultimaVigente = spark.read.format("delta").load(rutaUltimaVigente)
      .select("timestamp",
        "numerooperacion",
        "ejercicio",
        "periodicidad",
        "periodo",
        "tipodeclaracion",
        "tipocomplementaria",
        "identificadordeclaracion",
        "identificadordeclaracionpadre",
        "identificadordeclaracionraiz",
        "idejecucion",
        "p_timestamp")
    val ultimaVigenteDet = spark.read.format("delta").load(rutaUltimaVigenteDet)
      .select("partitionkey",
        "rowkey",
        "concepto",
        "rfc",
        "fechadeclaracion",
        "estatusdeclaracion",
        "identificadordeclaracion"
      )

    val resultJoin = ultimaVigente.as("A").join(ultimaVigenteDet.as("B"), Seq("identificadordeclaracion"), "inner")
      .select(
        "B.partitionkey",
        "B.rowkey",
        "A.timestamp",
        "B.concepto",
        "B.rfc",
        "A.numerooperacion",
        "B.fechadeclaracion",
        "A.ejercicio",
        "A.periodicidad",
        "A.periodo",
        "A.tipodeclaracion",
        "A.tipocomplementaria",
        "B.estatusdeclaracion",
        "A.identificadordeclaracion",
        "A.identificadordeclaracionpadre",
        "A.identificadordeclaracionraiz",
        "A.idejecucion",
        "A.p_timestamp").distinct()
    //    spark
    //      .sql(s"""SELECT B.partitionkey, B.rowkey, A.timestamp, B.concepto, B.rfc, A.numerooperacion, B.fechadeclaracion,
    //              |A.ejercicio, A.periodicidad, A.periodo, A.tipodeclaracion, A.tipocomplementaria, B.estatusdeclaracion, A.identificadordeclaracion,
    //              |A.identificadordeclaracionpadre, A.identificadordeclaracionraiz, A.idejecucion, A.p_timestamp
    //              |FROM ${rutaUltimaVigente} A
    //              |INNER JOIN  ${rutaUltimaVigenteDet} B
    //              |ON A.identificadordeclaracion = B.identificadordeclaracion""".stripMargin)

    resultJoin.show(50, false)
//    println(resultJoin.count())

  }
}
