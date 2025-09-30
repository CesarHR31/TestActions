package sat.diot.desdoble

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import sat.diot.comunes._
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal, EnumPlata}
import sat.diot.infraestructura.tablas._
import sat.diot.parser.{Bronce, EjecutorScripts, JsonToCaseClasses}
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

class DesdobleLocal extends UnitSpecWithDatabase
  with DockerJDBCSpec
  with PostgresAndScripts
  with DatabricksSecretsSpec {

  import spark.implicits._

  val idBatch: String = java.util.UUID.randomUUID().toString
  val fechaInicial: Timestamp = Timestamp.valueOf("2021-10-01 00:00:00")
  val fechaFinal: Timestamp = Timestamp.valueOf("2021-11-25 00:00:00")
  val formatIngesta = new SimpleDateFormat("yyyyMMddHHmmss")
  val formatFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private lazy val sampleFilesBasePathPlata = ConfigurationProvider.pathScriptsLlenadoPlata
  private lazy val sampleFilesBasePathOro = ConfigurationProvider.pathScriptsLlenadoOro
  private val ScopeName = ConfigurationProviderSecretsLocal.scopeName()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    secretsMap += ((ScopeName, "jdbc-postgresql-control-url") -> getJdbcUrl)
    secretsMap += ((
      ScopeName,
      "azure-conn-string-aplicativo"
    ) -> ConfigurationProviderSecretsLocal.azureConnStringAplicativo())
    secretsMap += ((
      ScopeName,
      "azure-conn-string-aplicativo-uat"
    ) -> ConfigurationProviderSecretsLocal.azureConnStringAplicativoUAT())

    val slickDatabase = Database.forURL(getJdbcUrl)
    val cuentas = TableQuery[PostgresAzureAccountTables]

    slickDatabase.run(
      cuentas ++= Seq(
        PostgresAzureAccountTable(
          "nombredoc",
          "id",
          "eu2dypdevstaentregadwh",
          "contenedorenviodwhdecisrmorjson",
          ConfigurationProviderSecretsLocal.endPointSASDEV(),
          "vigencia",
          Util.obtenerTimestamp()
        ),
        PostgresAzureAccountTable(
          "nombredoc",
          "id",
          "eu2dypuatstaentregadwh",
          "contenedorenviodwhdecisrmorjson",
          ConfigurationProviderSecretsLocal.endPointSASUAT(),
          "vigencia",
          Util.obtenerTimestamp()
        )
      )
    )
  }

  test("nada") {
    println("---->")
  }

  test("FullDesdoble") {

    val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
    val directoryPath = "C:\\tmp\\json\\diot\\"
    val id_ejecucion = "1234567890-1234567890-1234567802"
    val fechaPresentacion = Timestamp.valueOf("2024-02-05 01:00:00")
    val fechaInicio = "2024-02-05 00:00:00"
    val fechaFin = "2024-02-6 02:00:00"

    val outputTableName = ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name
    val data = SparkUtils.obtenerClavesSat(
      ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name.split("\\.")(1),
      ConfigurationProvider.identificadorBaseBronce
    )

    new File(directoryPath)
      .listFiles((_, name) => name.endsWith(".json"))
      .map { file =>
        logger.info(s"Procesando ${file.getAbsolutePath}")
        val fileContents = Util.getFileContents(file.getAbsolutePath)
        val jsonToCaseClasses = new JsonToCaseClasses(data)
        val parserResults = jsonToCaseClasses.processSmallJson(fileContents)

        Bronce(
          file.toString.split("\\.")(2).toLong,
          file.toString.split("\\.")(1),
          fechaPresentacion, //Util.obtenerTimestamp(),
          id_ejecucion,
          file.getAbsolutePath,
          declaracionValida = true,
          Util.obtenerTimestamp(),
          jsonToCaseClasses.obtenerErrores.asScala.toArray,
          parserResults.toOption
        )

      }
      .toSeq
      .toDS()
      .withColumn("p_fechapresentacion", expr("to_date(date_trunc('MM', fechaPresentacion))"))
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .saveAsTable(outputTableName)


    println("-----> Bronce")
    spark.table(outputTableName).show(80)


    new EjecutorScripts(sampleFilesBasePathPlata)
      .procesarTablas(
        id_ejecucion,
        List(0L),
        fechaInicio,
        fechaFin,
        esReproceso = false
      )


    println("--- plata identificadorTablaPlata_diot_decinfopeter")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name).show()
    println("--- plata identificadorTablaPlata_diot_inftotimpiva")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_inftotimpiva).name).show()
    println("--- plata identificadorTablaVigenciaPlata")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name).show()


    new EjecutorScripts(sampleFilesBasePathOro)
      .procesarTablas(
        id_ejecucion,
        List(0L),
        fechaInicio,
        fechaFin,
        esReproceso = false
      )


    println("--- Oro identificadorTablaOro_decinfopeter")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name).show()
    println("--- Oro identificadorTablaOro_infterdetiva")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_infterdetiva).name).show()
    println("--- Oro identificadorTablaOro_inftotimpiva")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_inftotimpiva).name).show()

    println("Termino!!!")
  }
}
