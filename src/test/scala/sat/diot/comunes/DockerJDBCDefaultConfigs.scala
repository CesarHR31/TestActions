package sat.diot.comunes

import java.util.Locale

object DockerJDBCDefaultConfigs {
  var DEFAULT_USER_NAME = "testusr"
  var DEFAULT_PASSWORD = "megaRandomPassw0rd!"
  var DEFAULT_DB_NAME = "testdb"

  def postgresql11(
                    userName: String = DEFAULT_USER_NAME,
                    password: String = DEFAULT_PASSWORD,
                    dbName: String = DEFAULT_DB_NAME,
                    exposeDefaultPort: Boolean = false
                  ): DatabaseOnDocker =
    new DatabaseOnDocker {
      override val useSameJdbcPort: Boolean = exposeDefaultPort
      override val imageName: String = "postgres:11.11"
      override val env: Map[String, String] = Map(
        "POSTGRES_USER" -> userName,
        "POSTGRES_PASSWORD" -> password,
        "POSTGRES_DB" -> dbName
      )
      override val jdbcPort: Int = 5432
      override val containerName: String =
        "POSTGRESQL11-" + java.util.UUID.randomUUID().toString.toUpperCase(Locale.ROOT)

      override def getJdcbUrl(ip: String, port: Int): String = {
        s"jdbc:postgresql://$ip:$port/$dbName?user=$userName&password=$password"
      }
    }

  def sqlServer2017(password: String = DEFAULT_PASSWORD, exposeDefaultPort: Boolean = false): DatabaseOnDocker = {
    new DatabaseOnDocker {
      override val useSameJdbcPort: Boolean = exposeDefaultPort
      override val imageName: String = "mcr.microsoft.com/mssql/server:2017-latest"
      override val env: Map[String, String] = Map(
        "ACCEPT_EULA" -> "Y",
        "SA_PASSWORD" -> password
      )
      override val jdbcPort: Int = 1433
      override val containerName: String =
        "SQLSERVER2017-" + java.util.UUID.randomUUID().toString.toUpperCase(Locale.ROOT)

      override def getJdcbUrl(ip: String, port: Int): String = {
        s"jdbc:sqlserver://$ip:$port;databaseName=master;user=sa;password=$password"
      }
    }

  }
}
