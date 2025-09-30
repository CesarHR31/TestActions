package sat.diot.ingesta.core

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.{EnumLand, SparkSessionManager}
import sat.diot.ingesta.core.Excepciones.{LandDFException, LandException}
import sat.diot.ingesta.entities.EsquemasCapas.Land
import sat.diot.ingesta.entities.WATDescarga

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable.ListBuffer

object ProcesamientoCapas {

  lazy val spark: SparkSession = SparkSessionManager.session
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  import spark.implicits._

  def creacionLand(resultadoWAT: java.lang.Iterable[WATDescarga], idEjecucion: String): List[Land] = {

    try {
      logger.info("Iniciada creacion de modelo Land")
      val resultadoLand = resultadoWAT.asScala.toList.map(a => {
        val fechaDeclaracion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        fechaDeclaracion.setTimeZone(TimeZone.getTimeZone("UTC"))
        val fechaDeclaracionFormato = fechaDeclaracion.format(a.getFechaDeclaracion)
        Land(
          a.getPartitionKey,
          idEjecucion,
          a.getRfc,
          a.getNumeroOperacion,
          a.getObligaciones,
          Timestamp.valueOf(fechaDeclaracionFormato),
          a.getEjercicio
        )
      })
      logger.info("Modelo land creado correctamente")
      resultadoLand
    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error al crear Land, error", e)
        throw LandException(e.getMessage, e)
    }
  }

  def recuperarTablas(baseDatos: String): List[String] = {
    val listaTablas =
      spark.catalog.listTables(baseDatos).select("name").rdd.map(r => s"$baseDatos." + r(0).toString).collect().toList
    listaTablas
  }

  def obtenerFiltrosReproceso(land: List[Land]): (List[Long], List[String]) = {
    var numOps     = ListBuffer.empty[Long]
    var fechasPres = ListBuffer.empty[Timestamp]
    land.foreach(land => {
      fechasPres += land.fechadeclaracion
      numOps += land.numerooperacion
    })

    val fechaTrunc = fechasPres
      .toDF("c0")
      .withColumn("c0", date_trunc("MM", col("c0")))
      .select("c0")
      .rdd
      .map(r => r(0).toString)
      .collect()
    (numOps.toList, fechaTrunc.toList.distinct)
  }

  def creacionDFLand(resultadoLand: List[Land], idEjecucion: String, urlCuentaStorage: String): sql.DataFrame = {

    try {
      import spark.implicits._
      logger.info("Iniciada creacion de Dataframe de Land")

      val tablaLandCompleta = resultadoLand
        .toDF()
        .withColumnRenamed("idejecucion","idEjecucion")
        .withColumn("blobpath", concat(lit(urlCuentaStorage), col("Rfc"), lit("."), col("NumeroOperacion"), lit(".json")))
        .withColumn("p_fechapresentacion", to_date(date_trunc("MM", col("FechaDeclaracion"))))

      tablaLandCompleta.write.mode(SaveMode.Append).format("delta").saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
      logger.info("Tabla Land creada correctamente")
      spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name).where($"idEjecucion" === lit(idEjecucion))

    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error al crear el Dataframe del Land", e)
        throw LandDFException(e.getMessage, e)
    }
  }
}
