package sat.diot.ultimoVigente

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, date_trunc, to_date}
import sat.diot.comunes._
import sat.diot.comunes.config.ConfigurationProvider.configs
import sat.diot.comunes.config.{ConfigurationProvider, ConfigurationProviderSecretsLocal, EnumPlata, SparkTable}
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.infraestructura.tablas._
import sat.diot.ingesta.entities.EsquemasCapas.UltimaVigenteLand
import sat.diot.parser.EjecutorScripts
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._

import java.sql.Timestamp

class FlujoUltimoVigente
  extends UnitSpecWithDatabaseUltimoVigente
    with DockerJDBCSpec
    with PostgresAndScripts
    with DatabricksSecretsSpec {

  private val ScopeName = ConfigurationProviderSecretsLocal.scopeName()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    secretsMap += ((ScopeName, "diot-jdbc-postgresql-control-url") -> getJdbcUrl)
    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion)

    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion2020"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2020)
    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion2021"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2021)
    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion2022"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2022)
    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion2023"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2023)

    secretsMap += ((
      ScopeName,
      "diot-connStrTableStorage-OrigenListadoRecepcion2024"
    ) -> ConfigurationProviderSecretsLocal.storageConnStringListadosRecepcion2024)

    secretsMap += ((
      ScopeName,
      "diot-sasBlobStorage-OrigenJsonDescargas"
    ) -> ConfigurationProviderSecretsLocal.sasBlobStorageOrigenJsonDescargas)


    val slickDatabase = Database.forURL(getJdbcUrl)

    val cuentas = TableQuery[PostgresAzureAccountTables]
    val wats2 = TableQuery[ConfiguracionWATDeclaracionAnualTable]

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
      wats2 ++= Seq(
        ConfiguracionWATDeclaracionAnual("DeclaracionConcepto", "DeclaracionConceptoDetalle", "diot-connStrTableStorage-OrigenListadoRecepcion2024", ScopeName, vigente = false, Util.obtenerTimestamp(), Util.obtenerTimestamp()),
        ConfiguracionWATDeclaracionAnual("DeclaracionConcepto", "DeclaracionConceptoDetalle", "diot-connStrTableStorage-OrigenListadoRecepcion2020", ScopeName, vigente = false, Util.obtenerTimestamp(), Util.obtenerTimestamp()),
        ConfiguracionWATDeclaracionAnual("DeclaracionConcepto", "DeclaracionConceptoDetalle", "diot-connStrTableStorage-OrigenListadoRecepcion2021", ScopeName, vigente = false, Util.obtenerTimestamp(), Util.obtenerTimestamp()),
        ConfiguracionWATDeclaracionAnual("DeclaracionConcepto", "DeclaracionConceptoDetalle", "diot-connStrTableStorage-OrigenListadoRecepcion2022", ScopeName, vigente = false, Util.obtenerTimestamp(), Util.obtenerTimestamp()),
        ConfiguracionWATDeclaracionAnual("DeclaracionConcepto", "DeclaracionConceptoDetalle", "diot-connStrTableStorage-OrigenListadoRecepcion2023", ScopeName, vigente = true, Util.obtenerTimestamp(), Util.obtenerTimestamp())
      )
    )
  }

  import spark.implicits._

  test("Procesamiento ultima vigente land") {
    val pgHandler = new PostgresqlHandler(getJdbcUrl)
    val db = pgHandler.slickDatabase
    val esReproceso = false
    var id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase

    Util.declaracionVigenteLand(id_ejecucion)

    println("-->Tabla Concepto")
    spark.table("default.ultimavigenteland_diot_1").show()
    println("-->Tabla Concepto Detalle")
    spark.table("default.ultimavigenteland_diot_2").show()

    /*

        var fi = Timestamp.valueOf("2016-01-01 00:00:00")
        var ff = Util.obtenerTimestamp()
        var fechasDelta = Util.devuelveDeltasUltimaVigente()


        fi = if(fechasDelta.isEmpty) Timestamp.valueOf(ConfigurationProvider.identificadorFechaUltimaVigenciaOro) else Timestamp.valueOf("2023-05-10 00:00:00")
        ff = Util.obtenerTimestamp()


        spark.sql(
          s"""INSERT INTO ${ConfigurationProvider.identificadorTablaCtlVigencia}
                                 VALUES('${idEjecucion}', '${fi.toString}', '${ff.toString}', '${Util.obtenerTimestamp()}')""")

    // primera ejecucion
        ultimaDeclaracionVigenteLand.ultimaDeclaracionVigenteLandRun(fi, ff, idEjecucion)*/
    println(s"-->${ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name} contador:${spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).count()}")
    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).show()
    /*
    // segunda ejecución
        //Thread.sleep(1000*60*3)
        idEjecucion = java.util.UUID.randomUUID().toString.toUpperCase
        fechasDelta = Util.devuelveDeltasUltimaVigente()
        fi = fechasDelta.head.getTimestamp(1)
        ff = Util.obtenerTimestamp()
        spark.sql(
          s"""INSERT INTO ${ConfigurationProvider.identificadorTablaCtlVigencia}
                                 VALUES('${idEjecucion}', '${fi.toString}', '${ff.toString}', '${Util.obtenerTimestamp()}')""")
        ultimaDeclaracionVigenteLand.ultimaDeclaracionVigenteLandRun(fi, ff, idEjecucion)
        println(s"-->${ConfigurationProvider.obtenerTablaPorid(DomainTable.WAT_LAND)} contador:${spark.table(ConfigurationProvider.obtenerTablaPorid(DomainTable.WAT_LAND)).count()}")
        spark.table(ConfigurationProvider.obtenerTablaPorid(DomainTable.WAT_LAND)).show()

        spark.table(ConfigurationProvider.identificadorTablaCtlVigencia).orderBy("fechainicio").show
        assert(spark.table(ConfigurationProvider.obtenerTablaPorid(DomainTable.WAT_LAND)).count() === spark.table("default.ultimavigenteland_af2").count())
    */
  }
  private val FechaIni = "2022-05-10 00:00:00"
  private val FechaFin = "2022-05-11 00:00:00"
  test("test flujo completo vigencia") {
    println(s"land:${spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).count()} bronce:${spark.table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name).count()} plata:${spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name).count()} oro:${spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name).count()}")
    // Carga 1
    //Land
    var id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase
    val fi = Timestamp.valueOf(FechaIni)
    val ff = Timestamp.valueOf(FechaFin)

    Seq(
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 00:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10001L, Timestamp.valueOf("2022-05-10 00:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 01:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10002L, Timestamp.valueOf("2022-05-10 01:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 02:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10003L, Timestamp.valueOf("2022-05-10 02:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 03:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10004L, Timestamp.valueOf("2022-05-10 03:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 04:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10005L, Timestamp.valueOf("2022-05-10 04:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 05:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10006L, Timestamp.valueOf("2022-05-10 05:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 06:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10007L, Timestamp.valueOf("2022-05-10 06:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 07:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10008L, Timestamp.valueOf("2022-05-10 07:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 08:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10009L, Timestamp.valueOf("2022-05-10 08:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 09:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10010L, Timestamp.valueOf("2022-05-10 09:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion)
    )
      .toDF
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
    println("-->Land")
    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).orderBy("numerooperacion").show()

    // bronce
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoBronce).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Bronce")
    spark.table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name).orderBy("numerooperacion").show()

    // plata
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoPlata).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Plata")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name).orderBy("numerooperacion").show()

    // oro
    spark.sql(s"DELETE FROM ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name}") // En el test es drop, pero en databricks debe de ser TRUNCATE
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoOro).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Oro")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name).orderBy("numerooperacion").show()

    println("------------------------------------------------------------------------------------------------------------------------")
    // Carga 2
    val fi1 = Timestamp.valueOf(FechaFin)
    val ff1 = Timestamp.valueOf("2022-05-12 00:00:00")
    id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase
    Seq(
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 00:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10004L, Timestamp.valueOf("2022-05-10 03:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 01:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10008L, Timestamp.valueOf("2022-05-10 07:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 02:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10009L, Timestamp.valueOf("2022-05-10 08:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 03:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10010L, Timestamp.valueOf("2022-05-10 09:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 04:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10011L, Timestamp.valueOf("2022-05-11 04:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 05:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10012L, Timestamp.valueOf("2022-05-11 05:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 06:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10013L, Timestamp.valueOf("2022-05-11 06:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 07:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10014L, Timestamp.valueOf("2022-05-11 07:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-11 08:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10015L, Timestamp.valueOf("2022-05-11 08:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion)
    )
      .toDF
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))
      .write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
    println("-->Land")
    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).orderBy("numerooperacion").select($"partitionkey", $"timestamp", $"numerooperacion", $"fechadeclaracion", $"estatusdeclaracion", $"idejecucion", $"p_timestamp").show()

    // bronce
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoBronce).procesarTablas(id_ejecucion, fi1.toString, ff1.toString)
    val contadorBronce = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND idejecucion IN('${id_ejecucion}')""").head.getLong(0)
    println(s"-->Bronce contador:${contadorBronce}")

    spark.table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name).orderBy("numerooperacion").select($"partitionkey", $"timestamp", $"numerooperacion", $"fechadeclaracion", $"estatusdeclaracion", $"idejecucion", $"p_timestamp").show()

    // plata
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoPlata).procesarTablas(id_ejecucion, fi1.toString, ff1.toString)
    val contadorPlata = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND idejecucion IN('${id_ejecucion}')""").head.getLong(0)
    println(s"s-->Plata contador;${contadorPlata}")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name).orderBy("numerooperacion").select($"partitionkey", $"timestamp", $"numerooperacion", $"fechadeclaracion", $"estatusdeclaracion", $"idejecucion", $"p_timestamp").show()

    // oro
    spark.sql(s"DELETE FROM ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name}") // En el test es drop, pero en databricks debe de ser TRUNCATE
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoOro).procesarTablas(id_ejecucion, fi1.toString, ff1.toString)
    val contadorOro = spark.sql(s"""SELECT COUNT(1) FROM ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name} WHERE (p_timestamp BETWEEN date_trunc('MM', '${fi.toString}') AND date_trunc('MM', '${ff.toString}')) AND (timestamp BETWEEN '${fi.toString}' AND '${ff.toString}')""").head.getLong(0)
    println(s"-->Oro contador:${contadorOro}")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name).orderBy("numerooperacion").select($"partitionkey", $"timestamp", $"numerooperacion", $"fechadeclaracion", $"estatusdeclaracion", $"idejecucion", $"p_timestamp").show()
  }
  test("test flujo repetidos") {
    var id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase
    val fi = Timestamp.valueOf(FechaIni)
    val ff = Timestamp.valueOf(FechaFin)

    // Se triplican para ver el comportamiento del merge en plata
    Seq(
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 00:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10001L, Timestamp.valueOf("2022-05-10 00:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 01:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10002L, Timestamp.valueOf("2022-05-10 01:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 02:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10003L, Timestamp.valueOf("2022-05-10 02:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 03:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10004L, Timestamp.valueOf("2022-05-10 03:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 04:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10005L, Timestamp.valueOf("2022-05-10 04:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 05:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10006L, Timestamp.valueOf("2022-05-10 05:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 06:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10007L, Timestamp.valueOf("2022-05-10 06:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 07:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10008L, Timestamp.valueOf("2022-05-10 07:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 08:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10009L, Timestamp.valueOf("2022-05-10 08:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 09:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10010L, Timestamp.valueOf("2022-05-10 09:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion)
    )
      .toDF
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))
      .write
      .mode(SaveMode.Append)
      .format("delta")
      .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)

    id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase
    Seq(UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 00:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10001L, Timestamp.valueOf("2022-05-10 00:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 01:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10002L, Timestamp.valueOf("2022-05-10 01:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 02:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10003L, Timestamp.valueOf("2022-05-10 02:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 03:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10004L, Timestamp.valueOf("2022-05-10 03:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 04:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10005L, Timestamp.valueOf("2022-05-10 04:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 05:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10006L, Timestamp.valueOf("2022-05-10 05:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 06:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10007L, Timestamp.valueOf("2022-05-10 06:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 07:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10008L, Timestamp.valueOf("2022-05-10 07:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 08:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10009L, Timestamp.valueOf("2022-05-10 08:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 09:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10010L, Timestamp.valueOf("2022-05-10 09:33:34"), 2022, null, null, null, null, 0, null, null, null, id_ejecucion))
      .toDF
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))
      .write
      .mode(SaveMode.Append)
      .format("delta")
      .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)

    id_ejecucion = java.util.UUID.randomUUID().toString.toUpperCase
    Seq(UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 00:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10001L, Timestamp.valueOf("2022-05-10 00:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 01:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10002L, Timestamp.valueOf("2022-05-10 01:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 02:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10003L, Timestamp.valueOf("2022-05-10 02:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 03:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10004L, Timestamp.valueOf("2022-05-10 03:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 04:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10005L, Timestamp.valueOf("2022-05-10 04:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 05:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10006L, Timestamp.valueOf("2022-05-10 05:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 06:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10007L, Timestamp.valueOf("2022-05-10 06:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 07:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10008L, Timestamp.valueOf("2022-05-10 07:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 08:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10009L, Timestamp.valueOf("2022-05-10 08:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion),
      UltimaVigenteLand("AACA830531UUA", "001", Timestamp.valueOf("2022-05-10 09:33:34"), "Obligaciones", "Concepto", "AACA830531UUA", 10010L, Timestamp.valueOf("2022-05-10 09:33:34"), 2022, null, null, null, null, 1, null, null, null, id_ejecucion))
      .toDF
      .withColumn("p_timestamp", to_date(date_trunc("MM", col("timestamp"))))
      .write
      .mode(SaveMode.Append)
      .format("delta")
      .saveAsTable(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
    println("-->Land")
    spark.table(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name).orderBy("numerooperacion").show()

    // bronce
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoBronce).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Bronce")
    spark.table(ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name).orderBy("numerooperacion").show(40)

    // plata
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoPlata).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Plata")
    spark.table(ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name).orderBy("numerooperacion").show(40)

    // oro
    spark.sql(s"DELETE FROM ${ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name}") // En el test es drop, pero en databricks debe de ser TRUNCATE
    new EjecutorScripts(ConfigurationProvider.pathScriptsLlenadoOro).procesarTablas(id_ejecucion, fi.toString, ff.toString)
    println("-->Oro")
    spark.table(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name).orderBy("numerooperacion").show()

  }

  test("scratch") {

    println(ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand))

    val algo = {
      configs.databases
        .flatMap { db =>
          db.tables.map {
            a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
          }.filter(_.name.contains(EnumDataBase.ctl_diot_conciliacion.toString))
        }

      println("-->")
    }
  }
}
