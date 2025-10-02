package sat.diot.general

import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.SparkSessionManager

import java.io.File
import java.nio.file.Paths

class ReadDeltaTableTest extends AnyFunSuite {
  private val spark = SparkSessionManager.getOrCreateLocal

  test("Working directory") {
    println("Working directory: " + Paths.get("").toAbsolutePath)
  }

  test("Read data from delta table") {
    val basePath = new File("").getAbsolutePath
    val relativePath = "diot_oro/diot_decinfopeter"
    val fullPath = new File(basePath, relativePath).getCanonicalPath
    val dfTemp = spark.read.format("delta").load(fullPath)
    dfTemp.show(false)
  }
}
