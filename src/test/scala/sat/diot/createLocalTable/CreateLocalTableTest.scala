package sat.diot.createLocalTable

import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.SparkSessionManager

import java.io.File


class CreateLocalTableTest extends AnyFunSuite {
  private val spark = SparkSessionManager.getOrCreateLocal

  test("Lee parquets oro local") {

    val bdDirectory = s"E:\\temp\\diot\\dec_inf_diotl"
    new File(bdDirectory)
      .listFiles()
      .map{table =>
        println(table.toString)

        spark.read
          .format("delta")
          .load(s"$table")
          .show(false)
      }
  }

}
