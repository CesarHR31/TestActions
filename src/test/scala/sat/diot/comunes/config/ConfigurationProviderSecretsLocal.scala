package sat.diot.comunes.config

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.google.gson.Gson
import sat.diot.comunes.Util

import java.nio.file.Paths


object ConfigurationProviderSecretsLocal extends Serializable {
  lazy val configs: ConfigurationDefinitionSecretsLocal = {
    val filePath = Paths.get("src", "test", "resources", "ValoresSecretsLocal.json")
      .toAbsolutePath
      .toString

    new Gson().fromJson(Util.getFileContents(filePath), classOf[ConfigurationDefinitionSecretsLocal])
  }

  def scopeName(): String = {
    configs.scopeName
  }

  def azureConnStringAplicativo(): String = {
    configs.azureConnStringAplicativo
  }

  def azureConnStringAplicativoUAT(): String = {
    configs.azureConnStringAplicativoUat
  }

  def endPointSASDEV(): String = {
    configs.endPointSASDEV
  }

  def endPointSASUAT(): String = {
    configs.endPointSASUAT
  }

  def storageConnStringListadosRecepcion: String = {
    configs.storageConnStringListadosRecepcion
  }
  def storageConnStringListadosRecepcion2020: String = {
    configs.storageConnStringListadosRecepcion2020
  }
  def storageConnStringListadosRecepcion2021: String = {
    configs.storageConnStringListadosRecepcion2021
  }
  def storageConnStringListadosRecepcion2022: String = {
    configs.storageConnStringListadosRecepcion2022
  }
  def storageConnStringListadosRecepcion2023: String = {
    configs.storageConnStringListadosRecepcion2023
  }
  def storageConnStringListadosRecepcion2024: String = {
    configs.storageConnStringListadosRecepcion2024
  }
  def storageConnStringListadosRecepcion2025: String = {
    configs.storageConnStringListadosRecepcion2025
  }
  def controlDWHServer: String = {
    configs.controlDWHServer
  }
  def controlDWHPuerto: String = {
    configs.controlDWHPuerto
  }
  def controlDWHBd: String = {
    configs.controlDWHBd
  }
  def controlDWHDIOTusr: String = {
    configs.controlDWHDIOTusr
  }
  def controlDWHDIOTpass: String = {
    configs.controlDWHDIOTpass
  }
  def sasBlobStorageOrigenJsonDescargas: String = {
    configs.sasBlobStorageOrigenJsonDescargas
  }
  def DWHDeclaracionesappInsights: String = {
    configs.DWHDeclaracionesappInsights
  }
  def URICuentaStorageArchivosJSON: String = {
    configs.diotSasBlobStorageOrigenJsonDescargasURL
  }


}
