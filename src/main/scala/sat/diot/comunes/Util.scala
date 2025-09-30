package sat.diot.comunes

import org.apache.spark.sql.{DataFrame, SparkSession}
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.core.IngestaCoord.jdbcUrl
import sat.diot.ingesta.core.ultimaDeclaracionVigenteLand
import sat.diot.ingesta.entities.RegistroMonitoreoNPSI
import sat.diot.parser.EjecutorScripts

import java.io.File
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.time._
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.UUID.randomUUID
import scala.util.Try

object Util {
  private def spark: SparkSession = SparkSessionManager.session

  private val StartSep = "Array("
  private val EndSep = ")"
  private val Separator = ", "
  lazy val postgresqlHandler = new PostgresqlHandler(jdbcUrl)
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  def obtenerTimestamp(zoneId: String = "America/Mexico_City"): java.sql.Timestamp = {
    val localDateTime = obtenerLocalDateTime(zoneId)
    java.sql.Timestamp.valueOf(localDateTime)
  }

  def obtenerLocalDateTime(zoneId: String = "America/Mexico_City"): LocalDateTime = {
    java.time.ZonedDateTime.now(java.time.ZoneId.of(zoneId)).toLocalDateTime
  }

  def generarUUID(): String = {
    randomUUID().toString
  }

  def getFileContents(path: String): String = {
    val scalaSource = scala.io.Source
      .fromFile(path)

    val fileContents = scalaSource.mkString

    scalaSource.close()

    fileContents
  }

  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"
                   ): File = {
    val dir = createDirectory(root, namePrefix)
    dir
  }

  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    val r = new File(root, namePrefix + "-" + UUID.randomUUID().toString)
    r.mkdirs()
    r
  }

  def directorioNoVacio(ruta: Path): Boolean = {
    var existe = false
    try {
      if (Files.isDirectory(ruta)) {
        val directorio = Files.newDirectoryStream(ruta)
        existe = directorio.iterator().hasNext
        directorio.close()
      } else {
        existe = false
      }
      existe
    } catch {
      case e: Exception =>
        println(e.getMessage)
        false
    }
  }

  def generarRangoPartitionKeyWAT(): (String, String) = {

    val dateFormat = "MM/dd/yyyy:HH"
    val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
    val dateString = LocalDateTime.now()
    val df = java.time.LocalDateTime.parse(dateString.toString, dtf)
    val di = java.time.LocalDateTime.parse(dateString.minusHours(2).toString, dtf)
    val rangoInicial =
      di.getYear.toString + di.getMonthValue.toString + di.getDayOfMonth.toString + di.getHour.toString + "0000"
    val rangoFinal =
      df.getYear.toString + df.getMonthValue.toString + df.getDayOfMonth.toString + df.getHour.toString + "0000"
    (rangoInicial, rangoFinal)
  }

  def eliminarRegistrosDeTabla(
                                nombreTabla: String,
                                fechaInicial: String,
                                fechaFinal: String,
                                listaNumerosOperacion: List[Long]
                              ): Unit = {

    logger.info(s"Eliminando registros de tabla: $nombreTabla")

    spark.sql(
      s"""DELETE FROM $nombreTabla A
                   WHERE A.p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                   AND date_trunc('MM','$fechaFinal')
                   AND A.numerooperacion IN(${listaNumerosOperacion.mkString(",")})""")
  }

  def obtenerInsumosPorNumeroOperacion(
                                        nombreTabla: String,
                                        fechaInicial: String,
                                        fechaFinal: String,
                                        listaNumerosOperacion: List[Long]
                                      ): DataFrame = {
    logger.info(s"Obtener insumos por número de operación de tabla: $nombreTabla")

    spark.sql(
      s"""SELECT * FROM $nombreTabla A
                   WHERE A.p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                   AND date_trunc('MM','$fechaFinal')
                   AND A.numerooperacion IN(${
        listaNumerosOperacion.mkString(",")
      })""")
  }

  def obtenerInsumos(
                      nombreTabla: String,
                      fechaInicial: String,
                      fechaFinal: String
                    ): DataFrame = {
    logger.info(s"Obtener insumos de tabla: $nombreTabla")
    spark.sql(
      s"""SELECT *
                            FROM (
                                  SELECT *
                                  FROM $nombreTabla
                                  WHERE p_fechapresentacion BETWEEN date_trunc('MM','$fechaInicial')
                                        AND date_trunc('MM','$fechaFinal')
                                  ) T1
                            WHERE T1.fechapresentacion >= '$fechaInicial'
                                  AND T1.fechapresentacion < '$fechaFinal'
                            """)
  }

  def obtenerNumeroCoresEnCluster(): Option[Int] = {
    Try({
      val workers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt
      val cores: Int = Runtime.getRuntime.availableProcessors
      workers * cores
    }).toOption
  }

  def declaracionVigenteLand(idEjecucion: String): Unit = {
    val inicioEjecucion = System.currentTimeMillis
    /* Inicio Proceso Vigentes*/
    try {
      val fechasDelta = Util.devuelveDeltasUltimaVigente()
      val fiVigente = if (fechasDelta.isEmpty) Timestamp.valueOf(ConfigurationProvider.identificadorFechaUltimaVigenciaOro) else fechasDelta.head.getTimestamp(1)
      val ffVigente = Util.obtenerTimestamp()
      println(s"Utlima Vigente Land, fechaInicio:$fiVigente fechaFinal:$ffVigente")
      /* Se inserta delta de ultima vigente*/
      spark.sql(s"""INSERT INTO ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name} VALUES('$idEjecucion', '${fiVigente.toString}', '${ffVigente.toString}', '${Util.obtenerTimestamp()}')""")
      /* Se trunca tabla de land ultima vigente*/
      spark.sql(s"TRUNCATE TABLE ${ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name}")
      ultimaDeclaracionVigenteLand.ultimaDeclaracionVigenteLandRun(fiVigente, ffVigente, idEjecucion)
      val contadorLandVigentes = spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).count()

      postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        idEjecucion,
        CatalogoPasoEjecucionNPSI.IntegracionLand,
        ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).idTabla,
        ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name,
        contadorLandVigentes,
        contadorLandVigentes,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
    }
    catch {
      case e: Exception =>
        logger.error(e)
        postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          idEjecucion,
          CatalogoPasoEjecucionNPSI.IntegracionLand,
          ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).idTabla,
          ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString(StartSep, Separator, EndSep)
        ))
    }
    /* Fin Proceso Vigentes*/
  }

  def devuelveDeltasUltimaVigente(): org.apache.spark.sql.DataFrame = {
    spark.sql(
      s"""SELECT fechainicio, fechafinal
                                                      FROM ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name}
                                                      WHERE fechafinal IN(SELECT MAX(fechafinal) fechafinal
                                                                                    FROM ${ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name})""")
  }

  def declaracionVigenteBronce(idEjecucion: String): Unit = {
    /* Inicio Proceso Vigentes*/
    val inicioEjecucion = System.currentTimeMillis
    try {
      logger.info("Iniciando proceso de vigentes en bronce")
      val fechasDelta = Util.devuelveDeltasUltimaVigente()
      val fi = fechasDelta.head.getTimestamp(0)
      val ff = fechasDelta.head.getTimestamp(1)

      new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoBronce).procesarTablas(idEjecucion, fi.toString, ff.toString)
      val contadorBronce = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND idejecucion IN('$idEjecucion')""").head.getLong(0)

      postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        idEjecucion,
        CatalogoPasoEjecucionNPSI.IntegracionBronce,
        ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).idTabla,
        ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name,
        contadorBronce,
        contadorBronce,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
    }
    catch {
      case e: Exception =>
        logger.error(e)
        postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          idEjecucion,
          CatalogoPasoEjecucionNPSI.IntegracionBronce,
          ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).idTabla,
          ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString(StartSep, Separator, EndSep)
        ))
    }
    /* Fin Proceso Vigentes*/
  }

  def declaracionVigentePlata(idEjecucion: String): Unit = {
    /* Inicio Proceso Vigentes*/
    val inicioEjecucion = System.currentTimeMillis
    try {
      logger.info("Iniciando proceso de vigentes en plata")
      val fechasDelta = Util.devuelveDeltasUltimaVigente()
      val fi = fechasDelta.head.getTimestamp(0)
      val ff = fechasDelta.head.getTimestamp(1)
      new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoPlata).procesarTablas(idEjecucion, fi.toString, ff.toString)
      val contadorPlata = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND idejecucion IN('$idEjecucion')""").head.getLong(0)

      postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        idEjecucion,
        CatalogoPasoEjecucionNPSI.IntegracionPlata,
        ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).idTabla,
        ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name,
        contadorPlata,
        contadorPlata,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
    }
    catch {
      case e: Exception =>
        logger.error(e)
        postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          idEjecucion,
          CatalogoPasoEjecucionNPSI.IntegracionPlata,
          ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).idTabla,
          ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString(StartSep, Separator, EndSep)
        ))
    }
    /* Fin Proceso Vigentes*/
  }

  def declaracionVigenteOro(idEjecucion: String): Unit = {
    /* Inicio Proceso Vigentes*/
    val inicioEjecucion = System.currentTimeMillis
    try {
      logger.info("Iniciando proceso de vigentes en oro")
      val fechasDelta = Util.devuelveDeltasUltimaVigente()
      val fi = fechasDelta.head.getTimestamp(0)
      val ff = fechasDelta.head.getTimestamp(1)
      spark.sql(s"TRUNCATE TABLE ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name}") // En el test es delete, pero en databricks debe de ser TRUNCATE
      new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoOro).procesarTablas(idEjecucion, fi.toString, ff.toString)
      val contadorOro = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND (timestamp BETWEEN '${fi.toString}' AND '${ff.toString}')""").head.getLong(0)

      postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
        idEjecucion,
        CatalogoPasoEjecucionNPSI.IntegracionOro,
        ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).idTabla,
        ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name,
        contadorOro,
        contadorOro,
        exitoso = true,
        Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
        null
      ))
    }
    catch {
      case e: Exception =>
        logger.error(e)
        postgresqlHandler.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          idEjecucion,
          CatalogoPasoEjecucionNPSI.IntegracionOro,
          ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).idTabla,
          ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString(StartSep, Separator, EndSep)
        ))
    }
    /* Fin Proceso Vigentes*/
  }
}
