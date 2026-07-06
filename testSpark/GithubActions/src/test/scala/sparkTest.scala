import comunes.SparkSessionManager
import org.scalatest.funsuite.AnyFunSuite

class sparkTest extends AnyFunSuite {

  val spark = SparkSessionManager.getOrCreateLocal

  import spark.implicits._

  test("Spark DF") {
    Seq(1, 2, 3, 4)
      .toDF()
      .show()
  }
}
