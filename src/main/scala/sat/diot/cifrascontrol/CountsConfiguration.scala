package sat.diot.cifrascontrol

import sat.diot.cifrascontrol.TipoConteoCifras.TipoConteoCifras

object CountsConfiguration {
  def countingInLayer(countsType: TipoConteoCifras): TCountsConfiguration = countsType match {
    case TipoConteoCifras.bronce => BronzeConfiguration
    case TipoConteoCifras.plata => SilverConfiguration
    case TipoConteoCifras.oro => GoldConfiguration
  }
}
