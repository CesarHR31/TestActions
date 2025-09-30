package sat.diot.flujobase

import com.azure.storage.blob._
import com.azure.storage.blob.specialized.BlobInputStream
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.config.ConfigurationProvider
import com.google.gson.{Gson, GsonBuilder}
import org.jsfr.json.provider.GsonProvider
import org.jsfr.json.{GsonParser, JsonSurfer}
import sat.diot.comunes.{DatabricksSecretsSpec, Util}
import sat.diot.infraestructura.tablas.{SecretsWatDeclaracionConceptoFechaTable, SecretsWatDeclaracionConceptoFechaTables}
import sat.diot.parser.{BlobContainerManager, IdentificacionDeLaDeclaracion}
import slick.jdbc.JdbcBackend.Database

import java.io.File
import java.net.URL

class BlobClientTest extends DatabricksSecretsSpec {

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    //secretsMap += (("diot", "diot-jdbc-postgresql-control-url") -> getJdbcUrl) //Cadena de conexión postgreSQL local
    //secretsMap += (("diot", "diot-connStrTableStorage-OrigenListadoRecepcion2024") -> "SharedAccessSignature=sv=2023-01-03&ss=t&srt=sco&st=2024-09-12T00%3A13%3A03Z&se=2024-10-01T00%3A13%3A00Z&sp=rwdxftlacup&sig=35OSpaMcfLPpG%2F4acK%2Brd%2B4H802zKpjahZcUCQ79CHU%3D;TableEndpoint=http://127.0.0.1:10002/diotdeclaradevlocal;") //Cuenta de lectura de listados
    secretsMap += (("diot", "diot-sasBlobStorage-OrigenJsonDescargas") -> "?sv=2018-03-28&st=2024-09-12T00%3A22%3A18Z&se=2024-10-01T00%3A22%3A00Z&sr=c&sp=rl&sig=B%2FfiPtpQYMX4wGSS6Qsqzhnw3HT8tTet8NJwDw2Os70%3D") //Cuenta para descarga de json


    /*Se estable el contexto para logger*/
    val context = org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
    val file = new File("H:\\Git\\SAT\\sat_diot_parser\\src\\main\\resources\\log4j2config.xml")
    context.setConfigLocation(file.toURI)
  }

  test("InputStream") {
    lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

    val blobpath = "http://127.0.0.1:10000/diotdecladescarga/contenedorenviodwhdecdiotjson/20231101.OWTR9030879R.179199048251.json"

    logger.info(blobpath.split("/").last)
    logger.info(s"${new URL(blobpath).getHost.split("\\.").head}") //UTILIZAR ESTA FORMA CUANDO SE TENGA LA URL DE APPS
    logger.info(s"${new URL(blobpath).toString.split("\\/")(3)}") //SE UTILIZA ESTA FORMA SOLO PARA PRUEBAS LOCALES

    val blobClient = new BlobContainerManager(blobpath.split("/").last,ConfigurationProvider.uriCuentaStorageArchivosJSON, ConfigurationProvider.sasBlobStorage)
    var fileInputStream: BlobInputStream = null

    fileInputStream = blobClient
      .obtenerInputStreamParaBlobEnCuenta
      .get

    val gson: Gson = new GsonBuilder()
      .setDateFormat("yyyy-mm-dd'T'hh:mm:ss")
      .create()

    val jSurfer = new JsonSurfer(GsonParser.INSTANCE, new GsonProvider(gson))
    val jsonCollector = jSurfer.collector(fileInputStream)

    val decla = jsonCollector.collectOne("$.IdentiDecla", classOf[IdentificacionDeLaDeclaracion])

    jsonCollector.exec()
    fileInputStream.close()

    logger.debug(decla.get)
  }
}
