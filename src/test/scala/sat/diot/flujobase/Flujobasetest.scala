package sat.diot.flujobase

import org.apache.logging.log4j.Logger
import sat.diot.cifrascontrol.{CifrasControlCoord, TipoConteoCifras}
import sat.diot.comunes._
import sat.diot.comunes.config.ConfigurationProvider.configs
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal, EnumPlata, SparkTable}
import sat.diot.conciliacion.{ConciliacionCoord, TipoConciliacion}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas.CifrasControlTable
import sat.diot.ingesta.core.IngestaCoord
import sat.diot.parser.{BronceCoord, OroCoord, PlataCoord}

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{ZoneId, ZonedDateTime}

class Flujobasetest extends UnitSpecWithDatabase
  with DockerJDBCSpec
  with PostgresAndScripts
  with DatabricksSecretsSpec {

  private val USER_DIR = s"${System.getProperty("user.dir")}"
  private val SCOPENAME = ConfigurationProviderSecretsLocal.scopeName()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    secretsMap += ((SCOPENAME, "controlDWH-server") -> ConfigurationProviderSecretsLocal.controlDWHServer)
    secretsMap += ((SCOPENAME, "controlDWH-puerto") -> ConfigurationProviderSecretsLocal.controlDWHPuerto)
    secretsMap += ((SCOPENAME, "controlDWH-bd") -> ConfigurationProviderSecretsLocal.controlDWHBd)
    secretsMap += ((SCOPENAME, "controlDWH-DIOTusr") -> ConfigurationProviderSecretsLocal.controlDWHDIOTusr)
    secretsMap += ((SCOPENAME, "controlDWH-DIOTpass") -> ConfigurationProviderSecretsLocal.controlDWHDIOTpass)
    secretsMap += ((SCOPENAME, "controlDWH-DIOTconnStr") -> getJdbcUrl) //Cadena de conexión postgreSQL local
    secretsMap += ((SCOPENAME, "diot-connStrTableStorage-OrigenListadoRecepcion") -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion) //Cuenta de lectura de listados
    secretsMap += ((SCOPENAME, "diot-connStrTableStorage-OrigenListadoRecepcion2022") -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2022) //Cuenta de lectura de listados
    secretsMap += ((SCOPENAME, "diot-connStrTableStorage-OrigenListadoRecepcion2023") -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2023) //Cuenta de lectura de listados
    secretsMap += ((SCOPENAME, "diot-connStrTableStorage-OrigenListadoRecepcion2025") -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2025) //Cuenta de lectura de listados
    secretsMap += ((SCOPENAME, "diot-sasBlobStorage-OrigenJsonDescargas") -> ConfigurationProviderSecretsLocal.sasBlobStorageOrigenJsonDescargas) //Cuenta para descarga de json
    secretsMap += ((SCOPENAME, "DWH-Declaraciones-appInsights") -> ConfigurationProviderSecretsLocal.DWHDeclaracionesappInsights)
    secretsMap += ((SCOPENAME, "diot-sasBlobStorage-OrigenJsonDescargasURL") -> ConfigurationProviderSecretsLocal.URICuentaStorageArchivosJSON)

    /*Se estable el contexto para logger*/
    val context = org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
    val file = new File(Paths.get("src", "main", "resources", "log4j2config.xml").toAbsolutePath.toString)//new File(s"$USER_DIR\\src\\main\\resources\\log4j2config.xml")
    context.setConfigLocation(file.toURI)
  }

  import spark.implicits._

  lazy val logger: Logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  test("lista de tablas") {
    val listaTablasOro: List[SparkTable] =
      configs.databases
        .flatMap { db =>
          db.tables.map {
              a =>
                println(s"${db.name}.${a.name}", a.id, a.idTabla)
                SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
            }
            .filter(_.name.contains(EnumDataBase.dec_inf_diotl.toString))
            .filter(!_.name.contains("land"))
            .filter(!_.name.contains("bronce"))
            .filter(!_.name.contains("plata"))
            .filter(!_.name.contains("ultimadeclaracion"))
            .filter(!_.name.toUpperCase.contains("VISTA"))
        }.toList

    listaTablasOro.toDF().show(false)

  }

  test("Obtiene URI Json Descarga") {
    println(ConfigurationProvider.uriCuentaStorageArchivosJSON)
    println(ConfigurationProvider.postgresqlControlUrl)
    val t = ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDateTime
    logger.info(s"Prueba de envío a Application Insights: $t")
  }

  test("Flujo Base") {
    val scriptsLlenadoOro = s"$USER_DIR\\src\\main\\resources\\databricks\\scripts-llenado\\oro"
    val scriptsLlenadoPlata = s"$USER_DIR\\src\\main\\resources\\databricks\\scripts-llenado\\plata"
    val listPrefix = Seq("01", "vigente")
    val esquemaOro = "dec_inf_diotl"
    val esquemaPlata = "diotl_plata"
    val rutaArchivosParquet = s"H:\\temp\\diot\\$esquemaOro"
    logger.info(ConfigurationProvider.applicationInsightsKey)

    val tmpDir: File = Paths.get(s"$USER_DIR\\consultaTerceros").toFile
    val bdDirPath = tmpDir.getAbsolutePath.replace("\\", "\\\\")
    val directoryBd = s"$bdDirPath\\\\$esquemaPlata.bd"

    val pgHandler = new PostgresqlHandler(getJdbcUrl)

    /*Se agregan estas líneas para saber como funciona la generación de fechas por defecto*/
    val t = ZonedDateTime.now(ZoneId.of("America/Mexico_City")).toLocalDateTime
    var fechaInicial = Timestamp.valueOf(t.minusHours(1).truncatedTo(ChronoUnit.HOURS))
    var fechaFinal = Timestamp.valueOf(t.minusHours(1 - 24).truncatedTo(ChronoUnit.HOURS))

    val infoBatch = new CifrasControlTable(
      idproc = Util.generarUUID().toUpperCase,
      fi = Timestamp.valueOf("2025-08-21 00:00:00"),
      ff = Timestamp.valueOf("2025-08-21 23:59:59"),
      ConfigurationProvider.identificadorWATConsulta,
      esReproceso = false,
      ConfigurationProvider.configs.storageControlConnString.scope,
      ConfigurationProvider.configs.storageControlConnString.key,
      ConfigurationProvider.uriCuentaStorageArchivosJSON
    )

    logger.info(infoBatch.id_ejecucion.toUpperCase)

    pgHandler.registrarNuevoBatch(infoBatch)

    IngestaCoord.procesaPendientes()

    //Util.declaracionVigenteLand(infoBatch.id_ejecucion)

    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
      .show(50, truncate = false)

    logger.info(getJdbcUrl)
    logger.info("Salida LAND: ")

    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
      .show(20, truncate = false)

    println(s"Registros en Land: ${spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name).count()}")

    new BronceCoord().procesaPendientes()

    //Util.declaracionVigenteBronce(infoBatch.id_ejecucion)

    println("Salida Bronce: ")

    spark
      .table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
      .where($"declaracionValida" === false)
      .show(20, truncate = false)
    spark
      .table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
      .where($"declaracionValida" === true)
      .show(20, truncate = false)

    new CifrasControlCoord().procesaPendientes(TipoConteoCifras.bronce)

    new PlataCoord().procesaPendientes()

    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name)
      .show(false)
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_infterdetiva).name)
      .show(false)
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_inftotimpiva).name)
      .show(false)

    new ConciliacionCoord().procesaPendientes(TipoConciliacion.plata)

    new CifrasControlCoord().procesaPendientes(TipoConteoCifras.plata)

    //Util.declaracionVigentePlata(infoBatch.id_ejecucion)

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $esquemaPlata LOCATION 'file:///$directoryBd'")
    spark.sql(s"USE $esquemaPlata")

    spark.sql(s"\nCREATE TABLE IF NOT EXISTS `$esquemaPlata`.`diot_oro_faltantes` (\n  `idejecucion` STRING,\n  `rfc` STRING,\n  `rfcdeclarante` STRING,\n  `numerooperacion` BIGINT,\n  `fechaIdentificacion` TIMESTAMP,\n  `estatus` SMALLINT)\nUSING delta;")

    new File(scriptsLlenadoPlata)
      .listFiles()
      .filter(n => !n.getName.toLowerCase().contains("vigente"))
      .foreach { file =>
        val nombreTabla = s"${file.getName.replace(".sql", "").split("\\-".toCharArray)(1)}_plata"
        println(s"Escribiendo en tabla $nombreTabla")
        spark.table(s"dec_inf_diotl_plata.$nombreTabla")
          .write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(s"$esquemaPlata.$nombreTabla")
        println(s"Fin de la escritura en tabla $esquemaPlata.$nombreTabla")
      }

    new OroCoord().procesaPendientes()

    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name)
      .show(50, truncate = false)
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_infterdetiva).name)
      .show(100, truncate = false)
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_inftotimpiva).name)
      .show(50, truncate = false)

    new File(scriptsLlenadoOro)
      .listFiles()
      .filter(f => !listPrefix.exists(p => f.getName.toLowerCase.contains(p)))
      .foreach { file =>
        val nombreTabla = file.getName.replace(".sql", "").split("-")(1)
        println(s"Escribiendo tabla $nombreTabla en ruta local: $rutaArchivosParquet")
        spark.table(s"$esquemaOro.$nombreTabla")
          .write
          .format("delta")
          .mode("overwrite")
          .save(s"$rutaArchivosParquet\\$nombreTabla")
        println(s"Fin de escritura en tabla $nombreTabla local: $rutaArchivosParquet")
      }

    new ConciliacionCoord().procesaPendientes(TipoConciliacion.oro)

    new CifrasControlCoord().procesaPendientes(TipoConteoCifras.oro)

    //Util.declaracionVigenteOro(infoBatch.id_ejecucion)

    println("Fin de la ejecución del proceso de desdoble.")
  }
}
