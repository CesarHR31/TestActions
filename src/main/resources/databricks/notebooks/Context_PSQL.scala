// Databricks notebook source
object  SqlNpsiCtlContext
{
  import java.util.Properties
  val jdbcHostname = dbutils.secrets.get("dwhPostgresKV","servidorPostgresControl")
  val jdbcPort = dbutils.secrets.get("dwhPostgresKV","puertoPostgres")
  val jdbcUsername = dbutils.secrets.get("dwhPostgresKV","usuarioPostgresControl")
  val jdbcPassword = dbutils.secrets.get("dwhPostgresKV","passPostgresControl")
  val jdbcDatabase = dbutils.secrets.get("dwhPostgresKV","baseDatosControl")
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}";
  val driverClass = "org.postgresql.Driver"
  Class.forName(driverClass)
  val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.put("password", jdbcPassword)
  connectionProperties.put("ssl","false")
  connectionProperties.put("sslmode","require")
  connectionProperties.setProperty("Driver", driverClass)
  val jdbcUrlCompleta = jdbcUrl + s"?user=${jdbcUsername}&password=${jdbcPassword}&sslmode=require"
  
  def aseguraConexionCtlNpsi(): Unit = {
    java.sql.DriverManager.getConnection(jdbcUrlCompleta, connectionProperties)
    println("PSQL Ctl OK!")
  }
}

// COMMAND ----------

SqlNpsiCtlContext.aseguraConexionCtlNpsi()

// COMMAND ----------

object  SqlNpsiCitusContext
{
  import java.util.Properties
  val jdbcHostname = dbutils.secrets.get("dwhPostgresKV","servidorPostgresDWH")
  val jdbcPort = dbutils.secrets.get("dwhPostgresKV","puertoPostgres")
  val jdbcUsername = dbutils.secrets.get("dwhPostgresKV","usuarioPostgresDWH")
  val jdbcPassword = dbutils.secrets.get("dwhPostgresKV","passPostgresDWH")
  val jdbcDatabase = dbutils.secrets.get("dwhPostgresKV","baseDatosDWH")
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}";
  val driverClass = "org.postgresql.Driver"
  Class.forName(driverClass)
  val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.put("password", jdbcPassword)
  connectionProperties.put("ssl","false")
  connectionProperties.put("sslmode","require")
  connectionProperties.setProperty("Driver", driverClass)
  val jdbcUrlCompleta = jdbcUrl + s"?user=${jdbcUsername}&password=${jdbcPassword}&sslmode=require"
  
  def aseguraConexionCitusNpsi(): Unit = {
    java.sql.DriverManager.getConnection(jdbcUrlCompleta, connectionProperties)
    println("PSQL Oro OK!")
  }
}

// COMMAND ----------

SqlNpsiCitusContext.aseguraConexionCitusNpsi()