package sat.diot.comunes

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}

import java.io.File

abstract class UnitSpecWithDatabaseUltimoVigente extends AnyFunSuite with BeforeAndAfterAll {
  private val UserDir = s"${System.getProperty("user.dir")}"
  System.setProperty(
    "log4j2.configurationFile",
    s"$UserDir\\src\\main\\resources\\log4j2config.xml"
  )

  SparkSessionManager.localSpark = true
  lazy val spark: SparkSession = SparkSessionManager.session

  val tmpDir: File       = Util.createTempDir()
  val tmpDirPath: String = tmpDir.getAbsolutePath.replace("\\", "\\\\")
  val dbName: String     = s"testing${java.util.UUID.randomUUID().toString.replaceAll("-", "")}"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $dbName LOCATION 'file:///$tmpDirPath\\\\testing.db'")

    val rutaArchivosDatabricks =
      s"$UserDir\\src\\main\\resources\\databricks\\tables"

    new File(rutaArchivosDatabricks)
      .list((dir: File, name: String) => {
        new File(dir, name).isDirectory
      })
      //.filter(p => p.toLowerCase().contains("vigente")) //--> quitar para que ejecute todas las capas
      .foreach { directory =>
        println(s"Creando db $directory")
        spark.sql(
          s"CREATE DATABASE IF NOT EXISTS $directory LOCATION 'file:///$tmpDirPath\\\\$directory.db'"
        )
          new File(s"$rutaArchivosDatabricks\\$directory")
            .listFiles()
            .filter(p => p.getAbsolutePath.toLowerCase().contains("vigente")) //--> quitar para que ejecute todas las capas
            .foreach { file =>
              println(s"Ejecutando ${file.getAbsolutePath}")
              spark.sql(s"USE $directory")
              spark.sql(
                Util
                  .getFileContents(file.getAbsolutePath).replace("\uFEFF", "")

                  // Vigencia
                  .replace("$ultimavigenteland$", ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorUltimaDeclaracionVigente_land).name)
                  .replace("$ultimavigentebronce$", ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorUltimaDeclaracionVigente_bronce).name)
                  .replace("$ultimavigenteplata$", ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaVigenciaPlata).name)
                  .replace("$ultimavigenteoro$", ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaVigenciaOro).name)
                  .replace("$controlultimavigente$", ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaCtlVigencia).name)
                  .replace("$partitionkeyrfcultimavigente$",ConfigurationProvider.identificadorTablaPartitionsKeysRfc)
                  .replace("$partitionkeyrfcobligacionesultimavigente$",ConfigurationProvider.identificadorTablaPartitionsKeysRfcObligaciones)

              )
            }

      }

    spark.sql("USE DEFAULT")
  }

  override protected def afterAll(): Unit = {

    spark.sql(s"DROP DATABASE $dbName CASCADE")
    val rutaArchivosDatabricks =
      s"$UserDir\\src\\main\\resources\\databricks\\tables"

    new File(rutaArchivosDatabricks)
      .list((dir: File, name: String) => {
        new File(dir, name).isDirectory
      })
      //.filter(p => p.toLowerCase().contains("vigente")) //--> quitar para que ejecute todas las capas
      .foreach { db => spark.sql(s"DROP DATABASE $db CASCADE") }

    FileUtils.deleteDirectory(tmpDir)

    val rutaSparkWarehouse = s"$UserDir\\spark-warehouse"

    FileUtils.deleteDirectory(new File(rutaSparkWarehouse))

    super.afterAll()
  }
}
