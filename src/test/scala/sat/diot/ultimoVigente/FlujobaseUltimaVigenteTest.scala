package sat.diot.ultimoVigente

import sat.diot.comunes.{DatabricksSecretsSpec, Util}
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal}
import slick.jdbc.JdbcBackend.Database

import java.io.File

class FlujobaseUltimaVigenteTest extends DatabricksSecretsSpec {

  private val userDir = s"${System.getProperty("user.dir")}"
  private val Scope = ConfigurationProviderSecretsLocal.scopeName()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    secretsMap += ((Scope, "controlDWH-server") -> ConfigurationProviderSecretsLocal.controlDWHServer)
    secretsMap += ((Scope, "controlDWH-puerto") -> ConfigurationProviderSecretsLocal.controlDWHPuerto)
    secretsMap += ((Scope, "controlDWH-bd") -> ConfigurationProviderSecretsLocal.controlDWHBd)
    secretsMap += ((Scope, "controlDWH-DIOTusr") -> ConfigurationProviderSecretsLocal.controlDWHDIOTusr)
    secretsMap += ((Scope, "controlDWH-DIOTpass") -> ConfigurationProviderSecretsLocal.controlDWHDIOTpass)
    secretsMap += ((Scope, "diot-connStrTableStorage-OrigenListadoRecepcion2025") -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2025) //Cuenta de lectura de listados
    secretsMap += ((Scope, "DWH-Declaraciones-appInsights") -> ConfigurationProviderSecretsLocal.DWHDeclaracionesappInsights)

    val slickDatabase = Database.forURL(ConfigurationProvider.postgresqlControlUrl)

    val context = org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
    val file = new File(s"$userDir\\src\\main\\resources\\log4j2config.xml")
    context.setConfigLocation(file.toURI)
  }

  test("Flujo base Ultima Vigente"){
    val idEjecucion = "C450B05F-1883-4700-930B-6EUATULTIVIG"
    Util.declaracionVigenteLand(idEjecucion)
  }
}
