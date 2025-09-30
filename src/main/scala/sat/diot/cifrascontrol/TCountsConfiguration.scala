package sat.diot.cifrascontrol

import sat.diot.comunes.EnumsTablas.{listaTablasOro, listaTablasPlata}
import sat.diot.comunes.{CatalogoPasoEjecucionNPSI, EnumBronce, EnumControl}
import sat.diot.comunes.config.{ConfigurationProvider, SparkTable}

/**
 * Trait sellado que define la configuración necesaria para ejecutar conteos de control
 * sobre una o más tablas.
 * Este trait sirve como contrato para las implementaciones que definen cómo y sobre qué
 * tablas se deben realizar conteos, así como dónde registrar los resultados.
 */
sealed trait TCountsConfiguration {

  /**
   * Secuencia de tablas sobre las cuales se realizarán los conteos.
   * Cada elemento representa una fuente de datos que será procesada.
   */
  def tablesSeq: Seq[SparkTable]

  /**
   * Nombre de la tabla de control destino donde se registrarán los resultados del conteo.
   */
  def targetControlTable: String

  /**
   * Enum de la capa donde se aplicará el conteo sobre las tablas.
   */
  def targetTypeControlCounts: TipoConteoCifras.Value

  /**
   * Identificador del paso de ejecución dentro de cada capa.
   */
  def executionStepId: Int

}

/**
 * Representa la configuración para la capa "Bronce" en el proceso de cifras.
 *
 * Este case object hereda de `TCountsConfiguration` y proporciona implemnentaciones especificas para:
 *   - La secuencia de tablas Spark que se procesarán (`tablesSeq`).
 *   - El nombre de la tabla de control de destino (`targetControlTable`), obtenido a través de ConfigurationProvider.
 *   - El tipo de conteo que se realizará, establecido en la capa bronce.
 *   - El identificador del paso de ejecución (`executionStepId`), correspondiente al paso de integración para la capa bronce.
 */
case object BronzeConfiguration extends TCountsConfiguration {

  private val bronzeTable = ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce)

  override def tablesSeq: Seq[SparkTable] = Seq(bronzeTable)

  override def targetControlTable: String = ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasBronce).name

  override def targetTypeControlCounts: TipoConteoCifras.Value = TipoConteoCifras.bronce

  override def executionStepId: Int = CatalogoPasoEjecucionNPSI.IntegracionBronce
}

/**
 * Representa la configuración para la capa "Plata" en el proceso de cifras.
 *
 * Este case object hereda de `TCountsConfiguration` y proporciona implemnentaciones especificas para:
 *   - La secuencia de tablas Spark que se procesarán (`tablesSeq`).
 *   - El nombre de la tabla de control de destino (`targetControlTable`), obtenido a través de ConfigurationProvider.
 *   - El tipo de conteo que se realizará, establecido en la capa plata.
 *   - El identificador del paso de ejecución (`executionStepId`), correspondiente al paso de integración para la capa plata.
 */
case object SilverConfiguration extends TCountsConfiguration {

  override def tablesSeq: Seq[SparkTable] = listaTablasPlata

  override def targetControlTable: String = ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasPlata).name

  override def targetTypeControlCounts: TipoConteoCifras.Value = TipoConteoCifras.plata

  override def executionStepId: Int = CatalogoPasoEjecucionNPSI.IntegracionPlata
}

/**
 * Representa la configuración para la capa "Oro" en el proceso de cifras.
 *
 * Este case object hereda de `TCountsConfiguration` y proporciona implemnentaciones especificas para:
 *   - La secuencia de tablas Spark que se procesarán (`tablesSeq`).
 *   - El nombre de la tabla de control de destino (`targetControlTable`), obtenido a través de ConfigurationProvider.
 *   - El tipo de conteo que se realizará, establecido en la capa oro.
 *   - El identificador del paso de ejecución (`executionStepId`), correspondiente al paso de integración para la capa oro.
 */
case object GoldConfiguration extends TCountsConfiguration {

  override def tablesSeq: Seq[SparkTable] = listaTablasOro

  override def targetControlTable: String = ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasOro).name

  override def targetTypeControlCounts: TipoConteoCifras.Value = TipoConteoCifras.oro

  override def executionStepId: Int = CatalogoPasoEjecucionNPSI.IntegracionOro
}

