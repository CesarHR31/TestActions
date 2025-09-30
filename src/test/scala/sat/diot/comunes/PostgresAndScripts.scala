package sat.diot.comunes

import java.io.File
import java.sql.Connection

trait PostgresAndScripts extends DockerJDBCSpec {

  override val db: DatabaseOnDocker = DockerJDBCDefaultConfigs
    .postgresql11(exposeDefaultPort = true)

  override def dataPreparation(connection: Connection): Unit = {

    val directoryPath = s"${System.getProperty("user.dir")}\\src\\main\\resources\\postgresql"

    new File(directoryPath)
      .listFiles((_, name) => name.endsWith(".sql"))
      .foreach { file =>
        println(s"Procesando ${file.getAbsolutePath}")
        val contents = Util.getFileContents(file.getAbsolutePath)
        connection.prepareStatement(contents).execute()
      }
  }
}
