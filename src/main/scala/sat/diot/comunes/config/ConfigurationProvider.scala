package sat.diot.comunes.config

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.google.gson.Gson
import sat.diot.comunes.EnumLand.EnumLand
import sat.diot.comunes.EnumControl.EnumControl
import sat.diot.comunes.EnumBronce.EnumBronce
import sat.diot.comunes.EnumOro.EnumOro
import sat.diot.comunes.EnumConciliacion.EnumConciliacion
import sat.diot.comunes.EnumStaging.EnumStaging
import sat.diot.comunes.config.EnumDefault.EnumDefault
import sat.diot.comunes.config.EnumPlata.EnumPlata
import sat.diot.comunes.{EnumDataBase, Util}

object ConfigurationProvider extends Serializable {

  private val userDir = s"${System.getProperty("user.dir")}"
  lazy val configs: ConfigurationDefinition = {
    val filePath = sys.env.getOrElse(
      "DIOT_CONFIGURATION_FILE_PATH",
      s"$userDir\\src\\main\\resources\\databricks\\code-config_diot.json"
    )

    new Gson().fromJson(Util.getFileContents(filePath), classOf[ConfigurationDefinition])
  }

  configs.databases.flatMap(db => {
    db.name
  })

  //---------------------------------------------------------
  def postgresqlDWHControlUrl: String = {
    val jdbcPostgresqlControl = "jdbc:postgresql://{servidorPostgres}:{puertoPostgres}/{baseDatos}?user={usuarioPostgres}&password={passPostgres}&sslmode=require"

    jdbcPostgresqlControl
      .replace("{servidorPostgres}", dbutils.secrets.get(configs.controlServidor.scope, configs.controlServidor.key))
      .replace("{puertoPostgres}", dbutils.secrets.get(configs.controlPuerto.scope, configs.controlPuerto.key))
      .replace("{baseDatos}", dbutils.secrets.get(configs.controlBd.scope, configs.controlBd.key))
      .replace("{usuarioPostgres}", dbutils.secrets.get(configs.controlUsuario.scope, configs.controlUsuario.key))
    .replace("{passPostgres}", dbutils.secrets.get(configs.controlPass.scope, configs.controlPass.key))
  }

  def postgresqlControlUrl: String = {
//    dbutils.secrets.get(configs.postgresqlControlUrl.scope, configs.postgresqlControlUrl.key)
  "jdbc:postgresql://localhost:5432/testdb?user=testusr&password=megaRandomPassw0rd!"
  }

  def postgresqlCitusUrl: String = {
    val jdbcPostgres = "jdbc:postgresql://{servidorPostgres}:{puertoPostgres}/{baseDatos}?user={usuarioPostgres}&password={passPostgres}&sslmode=require"

    jdbcPostgres
      .replace("{servidorPostgres}", dbutils.secrets.get(configs.citusServidor.scope, configs.citusServidor.key))
      .replace("{puertoPostgres}", dbutils.secrets.get(configs.citusPuerto.scope, configs.citusPuerto.key))
      .replace("{usuarioPostgres}", dbutils.secrets.get(configs.citusUsuario.scope, configs.citusUsuario.key))
      .replace("{passPostgres}", dbutils.secrets.get(configs.citusPass.scope, configs.citusPass.key))
      .replace("{baseDatos}", dbutils.secrets.get(configs.citusBd.scope, configs.citusBd.key))
  }

  def storageControlConnString: String = {
    dbutils.secrets
      .get(configs.storageControlConnString.scope, configs.storageControlConnString.key)
  }

  def storageControlConnStringReprocesos: String = {
    dbutils.secrets
      .get(configs.storageControlConnStringReprocesos.scope, configs.storageControlConnStringReprocesos.key)
  }

  def storageControlConnStringUAT: String = {
    println(userDir)
    dbutils.secrets
      .get(configs.storageControlConnStringUAT.scope, configs.storageControlConnStringUAT.key)
  }

  def sasBlobStorage: String = {
    dbutils.secrets
      .get(configs.sasBlobStorage.scope, configs.sasBlobStorage.key)
  }

  def identificadorBaseBronce: String = {
    configs.identificadorBaseBronce
  }

  def identificadorBasePlata: String = {
    configs.identificadorBasePlata
  }

  def identificadorBaseOro: String = {
    configs.identificadorBaseOro
  }

  def identificadorBaseLand: String = {
    configs.identificadorBaseLand
  }

  def identificadorBaseControl: String = {
    configs.identificadorBaseControl
  }


  def uriCuentaStorageArchivosJSON: String = {
    //configs.URICuentaStorageArchivosJSON
    dbutils.secrets.get(configs.URICuentaStorageArchivosJSON.scope, configs.URICuentaStorageArchivosJSON.key)
  }

  def identificadorWATConsulta: String = {
    configs.identificadorWATConsulta
  }

  def pathScriptsLlenadoBronce: String = {
    sys.env.getOrElse(
      "DIOT_SCRIPTS_FILES_FILL_PATH",
      s"$userDir\\src\\main\\resources\\databricks\\scripts-llenado\\"
    ) + "bronce"
  }

  def pathScriptsLlenadoPlata: String = {
    sys.env.getOrElse(
      "DIOT_SCRIPTS_FILES_FILL_PATH",
      s"$userDir\\src\\main\\resources\\databricks\\scripts-llenado\\"
    ) + "plata"
  }

  def pathScriptsLlenadoOro: String = {
    sys.env.getOrElse(
      "DIOT_SCRIPTS_FILES_FILL_PATH",
      s"$userDir\\src\\main\\resources\\databricks\\scripts-llenado\\"
    ) + "oro"
  }

  def applicationInsightsKey: String = {
    dbutils.secrets.get(configs.applicationInsightsKey.scope, configs.applicationInsightsKey.key)
  }

  def nombreProyecto: String = {
    configs.proyecto
  }

  def mountCSVNPSI: String = {
    configs.mountCSVNPSI
  }

  def prefijoCsvDeltasOro: String = {
    configs.prefijoCsvDeltasOro
  }

  def obtenerNombreDB2ParaTabla(tabla: String): Option[String] = {
    if (configs.mapeoTablasDB2.containsKey(tabla)) {
      Some(configs.mapeoTablasDB2.get(tabla))
    } else {
      None
    }
  }

  def identificadorCapa: String = {
    configs.identificadorCapa
  }

  def identificadorAplicacion: String = {
    configs.identificadorAplicacion
  }

  def identificadorAmbiente: String = {
    configs.identificadorAmbiente
  }

  def identificadorCodigoAplicacion: String = {
    configs.identificadorCodigoAplicacion
  }

  def identificadorOrigen: String = {
    configs.identificadorOrigen
  }

  def identificadorPorcentajeCuarentenaPermitido: Int = {
    configs.identificadorPorcentajeCuarentenaPermitido
  }

  lazy val identificadorFechaUltimaVigenciaOro: String = {
    configs.identificadorFechaUltimaVigenciaOro
  }

  lazy val identificadorEsquemaCitus: String = {
    configs.identificadorEsquemaCitus
  }

  lazy val identificadorTablaUltimaDeclaracionVigente: String = {
    configs.identificadorEsquemaCitus + "." + configs.identificadorTablaUltimaDeclaracionVigente
  }

  lazy val identificadorIdTablaUltimaDeclaracionVigenteCitus: Int = {
    configs.identificadorIdTablaUltimaDeclaracionVigenteCitus
  }

  lazy val identificadorTablaPartitionsKeysRfc: String = {
    configs.identificadorBaseControl + "." + configs.identificadorTablaPartitionsKeysRfc
  }
  lazy val identificadorTablaPartitionsKeysRfcObligaciones: String = {
    configs.identificadorBaseControl + "." + configs.identificadorTablaPartitionsKeysRfcObligaciones
  }

  lazy val identificadorIdProceso: Int = {
    configs.identificadorIdProceso
  }

//  def tipoPersona: String = {
//    configs.tipoPersona
//  }

  def ejercicioDeclaracion: Int = {
    configs.ejercicioDeclaracion.toInt
  }

  def idEstadoProceso: Int = {
    configs.idEstadoProceso
  }

  def identificadorBaseStaging: String = {
    configs.identificadorBaseStaging
  }

  def identificadorBaseDiotConciliacion: String = {
    configs.identificadorBaseConciliacion
  }

  def identificadorPathCsvFaltantes: String = {
    configs.identificadorPathCsvFaltantes
  }

  def identificadorEsquemaBitacorasSingle: String = {
    configs.identificadorEsquemaBitacorasSingle
  }

  def identificadorTablaContadorReporte: String = {
    configs.identificadorEsquemaBitacorasSingle + "." + configs.identificadorTablaContadorReporte
  }

  def identificadorTablaContadorPSQLPostgreSQL: String = {
    configs.identificadorEsquemaBitacorasSingle + "." + configs.identificadorTablaContadorPSQLPostgreSQL
  }

  def identificadorTablaBitacoraReprocesosSingle: String = {
    configs.identificadorEsquemaBitacorasSingle + "." + configs.identificadorTablaBitacoraReprocesosSingle
  }

  def identificadorReintentosReprocesamiento: Int = {
    configs.identificadorReintentosReprocesamiento
  }

  def obtenerTablaLandId(id: EnumLand): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.dec_inf_diotl_land.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaControlId(id: EnumControl): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.diot_ctl.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaBronceId(id: EnumBronce): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.dec_inf_diotl_bronce.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaPlataId(id: EnumPlata): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.dec_inf_diotl_plata.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaOroId(id: EnumOro): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
            a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
          }.filter(_.name.contains(EnumDataBase.dec_inf_diotl.toString))
          .filter(!_.name.contains("land"))
          .filter(!_.name.contains("bronce"))
          .filter(!_.name.contains("plata"))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaConciliacionId(id: EnumConciliacion): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.ctl_diot_conciliacion.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaStagingId(id: EnumStaging): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.ctl_diot_staging.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerTablaDefaultId(id: EnumDefault): SparkTable = {
    configs.databases
      .flatMap { db =>
        db.tables.map {
          a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
        }.filter(_.name.contains(EnumDataBase.default.toString))
      }
      .filter(_.id == id.toString)
      .head
  }

  def obtenerLimiteArchivosNormales: Long = {
    configs.limiteArchivosNormales
  }

  def obtenerLimiteArchivosGrandes: Long = {
    configs.limiteArchivosGrandes
  }

  def obtenerObligaciones: String = {
    configs.obligaciones
  }

  def agrupacionSurferJson: Integer = {
    configs.agrupacionSurferJson
  }
  val firstDigitApplicationCode: String = identificadorCodigoAplicacion.substring(0, 1)
  val idNumericDataProcess: String = configs.idTipoProcesoDatos
}
