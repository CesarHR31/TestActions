package sat.diot.comunes

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}

import java.io.File
import java.nio.file.Paths

abstract class UnitSpecWithDatabase extends AnyFunSuite with BeforeAndAfterAll {
  private val UserDir = s"${System.getProperty("user.dir")}"
//  System.setProperty(
//    "log4j2.configurationFile",
//    s"$UserDir\\src\\test\\resources\\log4j2config.xml"
//  )

  SparkSessionManager.localSpark = true
  lazy val spark: SparkSession = SparkSessionManager.session
  var ScriptDataBricks: String =
    s"""-----------------------------------------bases de datos-----------------------------------------
       |CREATE DATABASE IF NOT EXISTS dec_inf_diotl_land LOCATION '/mnt/imp-internos-land/Informativas/Dec_terceros/Diot_linea/dec_inf_diotl_land.db';
       |CREATE DATABASE IF NOT EXISTS diot_ctl LOCATION '/mnt/imp-internos-land/Informativas/Dec_terceros/Diot_linea/diot_ctl.db';
       |CREATE DATABASE IF NOT EXISTS dec_inf_diotl_bronce LOCATION '/mnt/imp-internos-bronce/Informativas/Dec_terceros/Diot_linea/dec_inf_diotl_bronce.db';
       |CREATE DATABASE IF NOT EXISTS dec_inf_diotl_plata LOCATION '/mnt/imp-internos-plata/Informativas/Dec_terceros/Diot_linea/dec_inf_diotl_plata.db';
       |CREATE DATABASE IF NOT EXISTS dec_inf_diotl LOCATION '/mnt/imp-internos-oro/Informativas/Dec_terceros/Diot_linea/dec_inf_diotl.db';
       |CREATE DATABASE IF NOT EXISTS diot_deltas LOCATION '/mnt/imp-internos-oro/Informativas/Dec_terceros/Diot_linea/diot_deltas.db';
       |CREATE DATABASE IF NOT EXISTS ctl_diot_conciliacion LOCATION '/mnt/imp-internos-land/Informativas/Dec_terceros/Diot_linea/ctl_diot_conciliacion.db';
       |CREATE DATABASE IF NOT EXISTS ctl_diot_staging LOCATION '/mnt/imp-internos-land/Informativas/Dec_terceros/Diot_linea/ctl_diot_staging.db';""".stripMargin

  val tmpDir: File = Util.createTempDir()
  val tmpDirPath: String = tmpDir.getAbsolutePath.replace("\\", "\\\\")
  val dbName: String = s"testing${java.util.UUID.randomUUID().toString.replaceAll("-", "")}"
println("<<<" + tmpDirPath + ">>>")
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $dbName LOCATION 'file:///$tmpDirPath\\\\testing.db'")

    val rutaArchivosDatabricks = Paths.get("src", "main", "resources", "databricks", "tables")
      .toAbsolutePath
      .toString
      //s"$UserDir\\src\\main\\resources\\databricks\\tables"

    spark.sql(
      s"CREATE DATABASE IF NOT EXISTS default LOCATION 'file:///$tmpDirPath\\\\default.db'"
    )


    new File(rutaArchivosDatabricks)
      .list((dir: File, name: String) => {
        new File(dir, name).isDirectory
      })
      //.filter(p => p.contains("bronce") || p.contains("land") || p.contains("ctl")) //--> quitar para que ejecute todas las capas
      //.filter(p => !p.contains("oro")) // --> quitar para que genere el oro
      //.filter(p => !p.contains("plata")) // --> quitar para que genere el oro
      .foreach { directory =>
        println(s"Creando db $directory")
        spark.sql(
          s"CREATE DATABASE IF NOT EXISTS $directory LOCATION 'file:///$tmpDirPath\\\\$directory.db'"
        )

        ScriptDataBricks = ScriptDataBricks  + s"\n\n\r-----------------------------------------$directory-----------------------------------------\n\r"

        new File(s"$rutaArchivosDatabricks\\$directory")
          .listFiles()
          //.par
          //.filter(p => !p.getAbsolutePath.toLowerCase().contains("vigente")) //--> quitar para que ejecute todas las capas
          .foreach { file =>
            println(s"Ejecutando ${file.getAbsolutePath}")
            spark.sql(s"USE $directory")
            val  scriptEjecutar =

              Util
                .getFileContents(file.getAbsolutePath).replace("\uFEFF", "")

                //----------------------------------- ORO
                .replace("$diot_decinfopeter$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name)
                .replace("$diot_infterdetiva$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_infterdetiva).name)
                .replace("$diot_inftotimpiva$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_inftotimpiva).name)

                //------------------------- PLATA
//                .replace("$plata_decinfoperacionesterceros$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_decinfoperacionesterceros).name)
//                .replace("$plata_datosdeltercerodeclarado$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosdeltercerodeclarado).name)
//                .replace("$plata_ivadeclararterceroproveedor$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_ivadeclararterceroproveedor).name)
//                .replace("$plata_valoractacti$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_valoractacti).name)
//                .replace("$plata_ivaacreditable$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_ivaacreditable).name)
//                .replace("$plata_ivanoacreditable$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_ivanoacreditable).name)
//                .replace("$plata_datosadicionales$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosadicionales).name)
//                .replace("$plata_totales$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_totales).name)
//                .replace("$plata_infototreportados$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_infototreportados).name)
//                .replace("$plata_valactacti$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_valactacti).name)
//                .replace("$plata_ivaacreditabletot$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_ivaacreditabletot).name)
//                .replace("$plata_ivanoacreditabletot$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_ivanoacreditabletot).name)
//                .replace("$plata_datosadicionalestot$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_datosadicionalestot).name)
//                .replace("$plata_detdatosinformativos$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_detdatosinformativos).name)

                .replace("$plata_diot_decinfopeter$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name)
                .replace("$plata_diot_infterdetiva$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_infterdetiva).name)
                .replace("$plata_diot_inftotimpiva$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_inftotimpiva).name)

                //-------------------------
                .replace("$bronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name)
                .replace("$vistacuarentena$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaCuarentena).name)
                .replace("$vistabronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorVistaBronce).name)
                .replace("$cifrascontrolbronce$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasBronce).name)
                .replace("$cifrascontroloro$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasOro).name)
                .replace("$cifrascontrolplata$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCifrasPlata).name)
                .replace("$conciliacion$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaConciliacion).name)
                .replace("$land$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name)


                // Vigencia
                .replace("$ultimavigenteland$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
                .replace("$ultimavigentebronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name)
                .replace("$ultimavigenteplata$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name)
                .replace("$ultimavigenteoro$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)
                .replace("$controlultimavigente$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name)
                .replace("$partitionkeyrfcultimavigente$", ConfigurationProvider.identificadorTablaPartitionsKeysRfc)
                .replace("$partitionkeyrfcobligacionesultimavigente$", ConfigurationProvider.identificadorTablaPartitionsKeysRfcObligaciones)

            spark.sql(scriptEjecutar)
            ScriptDataBricks = ScriptDataBricks + "\n\r" + scriptEjecutar
          }
      }

    spark.sql("USE DEFAULT")

    ScriptDataBricks = ScriptDataBricks + "\n\r" + """------------------------------------------< ctl_diot_conciliacion >---------------------------------------------
CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadorland (
    `nombreTabla`         STRING    NOT NULL COMMENT 'Nombre de la tabla en donde se realiza el conteo',
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `contador`            LONG      NOT NULL COMMENT 'Conteo del delta',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo',
    `tiempoProcesamiento` LONG      NOT NULL COMMENT 'Tiempo en minutos que duro el proceso de conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadorbronce (
    `nombreTabla`         STRING    NOT NULL COMMENT 'Nombre de la tabla en donde se realiza el conteo',
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `contador`            LONG      NOT NULL COMMENT 'Conteo del delta',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo',
    `tiempoProcesamiento` LONG      NOT NULL COMMENT 'Tiempo en minutos que duro el proceso de conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadorcuarentena (
    `nombreTabla`         STRING    NOT NULL COMMENT 'Nombre de la tabla en donde se realiza el conteo',
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `contador`            LONG      NOT NULL COMMENT 'Conteo del delta',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo',
    `tiempoProcesamiento` LONG      NOT NULL COMMENT 'Tiempo en minutos que duro el proceso de conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadorplata (
    `nombreTabla`         STRING    NOT NULL COMMENT 'Nombre de la tabla en donde se realiza el conteo',
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `contador`            LONG      NOT NULL COMMENT 'Conteo del delta',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo',
    `tiempoProcesamiento` LONG      NOT NULL COMMENT 'Tiempo en minutos que duro el proceso de conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadororo (
    `nombreTabla`         STRING    NOT NULL COMMENT 'Nombre de la tabla en donde se realiza el conteo',
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `contador`            LONG      NOT NULL COMMENT 'Conteo del delta',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo',
    `tiempoProcesamiento` LONG      NOT NULL COMMENT 'Tiempo en minutos que duro el proceso de conteo'
    )
USING DELTA;


CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.listadofechapresentacion (
  idEjecucion STRING,
  fecha DATE)
USING delta
PARTITIONED BY (fecha);

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.listadorecepcion (
  fechaPresentacion STRING,
  rfc STRING,
  numeroOperacion STRING,
  timestamp timestamp,
  blobpath STRING,
  p_fechaPresentacion DATE
  )
USING delta
PARTITIONED BY (p_fechaPresentacion);

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.contadorpostgresql (
  idejecucion STRING COMMENT 'Identificador de la Ejecución',
  nombreTabla STRING COMMENT 'Nombre de la tabla en donde se realiza el conteo',
  fechaPresentacion DATE COMMENT 'Fecha de presentación de la declaración',
  contadororo BIGINT COMMENT 'Conteo del delta en parquet Oro',
  contadornpsi BIGINT COMMENT 'Conteo del delta en PostgreSQL NPSI',
  estatus SMALLINT COMMENT 'Estado del delta, 1 sin diferencia, 2 con diferencia',
  fechaactualizacion TIMESTAMP COMMENT 'Fecha en la que se realiza el conteo',
  tiempoprocesamiento BIGINT COMMENT 'tiempoprocesamiento'
) USING DELTA
PARTITIONED BY (fechaPresentacion);

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.ContadorFechaPresentacion (
  fechaPresentacion Date,
  contador Long,
  fechaActualizacion timestamp,
  idEjecucion String,
  tiempoProcesamiento Long)
USING delta
PARTITIONED BY (fechaPresentacion);

CREATE TABLE IF NOT EXISTS ctl_diot_conciliacion.bitacora_contador_fecha
(
    fecha String,
    fechapresentacion Date,
    contadoraplicativo Bigint,
    contadordesdoble Bigint,
    diferencia Bigint,
    fechaactualizacion Timestamp,
    estado Int
) USING DELTA
PARTITIONED BY (fechapresentacion);

------------------------------------------< ctl_diot_staging >---------------------------------------------

CREATE TABLE IF NOT EXISTS ctl_diot_staging.faltantesland (
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `numeroOperacion`     LONG      NOT NULL COMMENT 'Número de operación de la declaración',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_staging.faltantesbronce (
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `numeroOperacion`     LONG      NOT NULL COMMENT 'Número de operación de la declaración',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_staging.faltantesplata (
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `numeroOperacion`     LONG      NOT NULL COMMENT 'Número de operación de la declaración',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_staging.faltantesoro (
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `numeroOperacion`     LONG      NOT NULL COMMENT 'Número de operación de la declaración',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo'
    )
USING DELTA;

CREATE TABLE IF NOT EXISTS ctl_diot_staging.faltantespostgresql (
    `fechaPresentacion`   DATE      NOT NULL COMMENT 'Fecha de presentación de la declaración',
    `numeroOperacion`     LONG      NOT NULL COMMENT 'Número de operación de la declaración',
    `fechaActualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza el conteo'
    )
USING DELTA;


CREATE TABLE IF NOT EXISTS ctl_diot_staging.ListadoUnicoReprocesos  (
  fechaPresentacion Date,
  numeroOperacion STRING,
  fechaActualizacion timestamp,
  blobpath STRING,
  estatus INT,
  archivoDestino STRING,
  reprocesos INT)
USING delta
PARTITIONED BY (fechaPresentacion);"""

    val deploymentRoute = Paths.get(s"$UserDir\\src\\main\\resources\\databricks\\despliegue\\databricks").toFile
    if(!deploymentRoute.exists())
      deploymentRoute.mkdirs()

    import java.io.PrintWriter
    new PrintWriter(s"$deploymentRoute\\01_crear_base_desdoble.sql") {
      write(ScriptDataBricks)
      close()
    }
  }

  override protected def afterAll(): Unit = {
    val rutaArchivosDatabricks =
      s"$UserDir\\src\\main\\resources\\databricks\\tables"
    println(rutaArchivosDatabricks)

        spark.sql(s"DROP DATABASE $dbName CASCADE")

       new File(rutaArchivosDatabricks)
          .list((dir: File, name: String) => {
            new File(dir, name).isDirectory
          })
          .foreach { db => spark.sql(s"DROP DATABASE $db CASCADE") }


        FileUtils.deleteDirectory(tmpDir)

        val rutaSparkWarehouse = s"$UserDir\\spark-warehouse"

        FileUtils.deleteDirectory(new File(rutaSparkWarehouse))

    super.afterAll()
  }
}
