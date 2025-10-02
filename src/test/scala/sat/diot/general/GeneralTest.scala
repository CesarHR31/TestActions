package sat.diot.general

import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.{SparkSessionManager, TSparkSession, Util}
import sat.diot.entities.SparkQueryEntity

import java.io.File
import java.nio.file.Paths

class GeneralTest extends AnyFunSuite  {
  private val spark = SparkSessionManager.getOrCreateLocal
  private val USER_DIR = s"${System.getProperty("user.dir")}"

  def cifrasControlQueryInputs(): Unit = {
    //    val queryObject = SparkQueryEntity(
    //      sourceTableName =  "dec_inf_diotl_bronce.bronce_diot",
    //      initDate = "2025-07-01 00:00:00",
    //      endDate = "2025-08-31 00:00:00",
    //      executionId ="695A56D0-62AB-45C8-9818-6FE9CE15F00F",
    //      targetTableName = "diot_ctl.cifrascontrolbronce",
    //      columnToDrop = "payload",
    //      columnToAdd = "fechainsercion"
    //    )
    println("Ejecución correcta!")
    println("Prueba desde [Git Hub Actions]")
    //    sparkQueryEntity = queryObject
  }

  def getTemplate: String = {
    val pathFile = Paths.get(USER_DIR, "src", "main", "resources", "databricks", "scripts-llenado", "scriptTest.sql").toFile
    val string = Util
      .getFileContents(pathFile.getAbsolutePath).replace("\uFEFF", "")

    string
  }

  test("Execute SQL query") {
    //    test_plata.diot_oro_faltantes
    //    println(getTemplate())
    //    val tmpDir: File = Paths.get(s"$USER_DIR\\consultaTerceros").toFile
    //    val bdDirPath = tmpDir.getAbsolutePath.replace("\\", "\\\\")
    //    val database = "test_plata"
    //    println(bdDirPath)
    //    val directoryBd = s"$bdDirPath\\\\$database.bd"
    //    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION 'file:///$directoryBd'")
    //    spark.sql(s"USE $database")
    //
    //    spark.sql(s"\nCREATE TABLE IF NOT EXISTS `$database`.`diot_oro_faltantes` (\n  `idejecucion` STRING,\n  `rfc` STRING,\n  `rfcdeclarante` STRING,\n  `numerooperacion` BIGINT,\n  `fechaIdentificacion` TIMESTAMP,\n  `estatus` SMALLINT)\nUSING delta;")
    //
    //    spark.table(s"$database.diot_oro_faltantes").show(false)

    cifrasControlQueryInputs()
  }

  test("Genera DF"){
    import spark.implicits._

    val seqNumbers = Seq(1, 2, 3, 4, 5)
    seqNumbers
      .toDF
      .show()
  }
}
