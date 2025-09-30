package sat.diot.comunes

import com.databricks.dbutils_v1._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

trait DatabricksSecretsSpec extends AnyFunSuite with BeforeAndAfterAll {

  val secretsMap: mutable.Map[(String, String), String] = scala.collection.mutable.Map[(String, String), String]()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(
      new DBUtilsV1 {
        override val widgets: WidgetsUtils = null
        override val meta: MetaUtils = null
        override val fs: DbfsUtils = null
        override val notebook: NotebookUtils = null
        override val secrets: SecretUtils = new SecretUtils {
          override def get(scope: String, key: String): String = {
            secretsMap((scope, key))
          }

          override def getBytes(scope: String, key: String): Array[Byte] = ???

          override def list(scope: String): Seq[SecretMetadata] = ???

          override def listScopes(): Seq[SecretScope] = ???

          override def help(): Unit = ???

          override def help(moduleOrMethod: String): Unit = ???
        }
        override val preview: Preview = null
        override val library: LibraryUtils = null
        override val credentials: DatabricksCredentialUtils = null

        override def help(): Unit = ???

        override def help(moduleOrMethod: String): Unit = ???

        override val data: DataUtils = null
        override val jobs: JobsUtils = null
      }
    )
  }
}
