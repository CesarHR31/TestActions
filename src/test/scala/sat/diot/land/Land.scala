package sat.diot.land


import sat.diot.comunes._
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.ingesta.core.IngestaCoord
import sat.diot.parser.BronceCoord
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._

import java.sql.Timestamp

class Land extends UnitSpecWithDatabase
  with DockerJDBCSpec
  with PostgresAndScripts
  with DatabricksSecretsSpec {

  private val ScopeName = ConfigurationProviderSecretsLocal.scopeName()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    secretsMap += (("diot", "diot-jdbc-postgresql-control-url") -> getJdbcUrl)
    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion)

    secretsMap += ((
      ScopeName,
      "diot-sasBlobStorage-OrigenJsonDescargas"
    ) -> ConfigurationProviderSecretsLocal.sasBlobStorageOrigenJsonDescargas)


    val slickDatabase = Database.forURL(getJdbcUrl)

    val cuentas = TableQuery[PostgresAzureAccountTables]
    val wats = TableQuery[SecretsWatDeclaracionConceptoFechaTables]

    slickDatabase.run(
      cuentas ++= Seq(
        PostgresAzureAccountTable(
          "nombredoc",
          "id",
          "wu1dandevstaentregadwh",
          "contenedorenviodwhdecanupmv2json",
          ConfigurationProviderSecretsLocal.endPointSASDEV(),
          "vigencia",
          Util.obtenerTimestamp()
        )
      )
    )
    slickDatabase.run(
      wats ++= Seq(
        SecretsWatDeclaracionConceptoFechaTable("DeclaracionAnualFecha", "diot-connStrTableStorage-OrigenListadoRecepcion", ScopeName, vigente = true, Util.obtenerTimestamp(), Util.obtenerTimestamp())
      )
    )
  }

  import spark.implicits._

  test("Procesamiento land") {

    val pgHandler = new PostgresqlHandler(getJdbcUrl)
    //val db = pgHandler.slickDatabase
    val esReproceso = false

    val infoBatch = new CifrasControlTable(
      idproc = java.util.UUID.randomUUID().toString.toUpperCase,
      fi = Timestamp.valueOf("2019-01-01 00:00:00"),
      ff = Timestamp.valueOf("2020-01-01 00:00:00"),
      ConfigurationProvider.identificadorWATConsulta,
      esReproceso,
      ConfigurationProvider.configs.storageControlConnString.scope,
      ConfigurationProvider.configs.storageControlConnString.key,
      ConfigurationProvider.uriCuentaStorageArchivosJSON
    )

    pgHandler.registrarNuevoBatch(infoBatch)


    //logger.info(Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf))

    IngestaCoord.procesaPendientes()
    println(s"conexión PSQL: $getJdbcUrl")
    println("Salida Land: ")

    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)
      //.where($"numerooperacion".isin("220310001892", "220420001417"))
      .show(50, truncate = false)

    println(s"Registros Land: ${spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name).count()}")

    /*-------------------------------------------------------------------------------------------------*/
    new BronceCoord().procesaPendientes()

    // logger.info(Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf))
    println("Salida Bronce: ")
    spark
      .table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
      .where($"declaracionValida" === false)
      .show(500, truncate = false)
    spark
      .table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
      .where($"declaracionValida" === true)
      .show(500, truncate = false)
    /*
        Await.result(db.run(pgHandler.tablaCifrasControl.result), Duration.Inf).foreach { r =>
          assert(r.idEstatus === CatalogoProceso.TERMINADOBRONCEEXITOSAMENTE)
        }*/
  }
}
