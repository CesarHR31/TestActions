package sat.diot.comunes

import org.apache.logging.log4j.Logger
import sat.diot.comunes.config.ConfigurationProvider

/**
 * Proporciona una instancia de Logging para usarla en clases que la hereden
 * con capacidades de registro en ApplicationInsights.
 */
trait TLogging {
  protected lazy val logger: Logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)
}
