package sat.diot.Logger


import applicationinsights.ApplicationInsightsAppender
import org.apache.logging.log4j.{LogManager, Logger}
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes._
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal}
import sat.diot.infraestructura.tablas.SecretsWatDeclaracionConceptoFechaTables
import slick.jdbc.JdbcBackend.Database

import java.io.File

class TestLogger extends DatabricksSecretsSpec  {

  private val ScopeName = ConfigurationProviderSecretsLocal.scopeName()
  private val userDir = s"${System.getProperty("user.dir")}"


  override protected def beforeAll(): Unit = {
    super.beforeAll()

    secretsMap += ((ScopeName, "DWH-Declaraciones-appInsights") -> ConfigurationProviderSecretsLocal.DWHDeclaracionesappInsights)


    /*Se estable el contexto para logger*/
    val context = org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
    val file = new File("src/main/resources/log4j2config.xml")
    context.setConfigLocation(file.toURI)
  }

  test("Logg Error") {
    lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

//    ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionOro
//    logger.debug("Test debug API")
//    ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionPlata
//    logger.info("Test info API")
//    ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro
//    logger.warn("test warn API")
//    ApplicationInsightsAppender.pasoEjecucion = CatalogoPasoEjecucionNPSI.IntegracionLand
//    logger.fatal("test fatal API")

  }
}

