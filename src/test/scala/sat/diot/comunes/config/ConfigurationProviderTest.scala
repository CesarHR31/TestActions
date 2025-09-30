package sat.diot.comunes.config

import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.EnumOro

class ConfigurationProviderTest extends AnyFunSuite{

  test("Tablas deafult temporales"){
    val tablaSize = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_size).name
    val tablaOroTemporal = ConfigurationProvider.obtenerTablaDefaultId(EnumDefault.identificadorTablaDefault_infterdetiva_temp).name

    println(s"$tablaSize\n$tablaOroTemporal")
  }

  test("Obtiene Secrets Locales"){
    println(ConfigurationProviderSecretsLocal.scopeName())
    println(ConfigurationProviderSecretsLocal.azureConnStringAplicativo())
    println(ConfigurationProviderSecretsLocal.azureConnStringAplicativoUAT())
    println(ConfigurationProviderSecretsLocal.endPointSASDEV())
    println(ConfigurationProviderSecretsLocal.endPointSASUAT())

    println(ConfigurationProviderSecretsLocal.URICuentaStorageArchivosJSON)
  }
}
