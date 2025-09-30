// Databricks notebook source
// MAGIC %run ./Comunes

// COMMAND ----------

import java.io.InputStream
import java.sql.DriverManager
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import sat.diot.comunes.config.ConfigurationProvider

// COMMAND ----------

object CopyHelper extends Serializable {

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.map { row =>
      (row.toSeq
        .map { v =>
          if (v == null) {
            """\N"""
          } else {
            "\"" + v.toString.replaceAll("\"", "\"\"") + "\""
          }
        }
        .mkString("\t") + "\n").getBytes
    }.flatten

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) {
          bytes.next & 0xff // make the signed byte an unsigned int
        } else {
          -1
        }
    }
  }

  def copyIn(url: String, df: org.apache.spark.sql.DataFrame, table: String):Unit = {
    df.rdd.foreachPartition { rows =>
      val conn = DriverManager.getConnection(url)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        cm.copyIn(
          s"COPY $table " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')""",
          rowsToInputStream(rows))
        ()
      } finally {
        conn.close()
      }
    }
  }
}

// COMMAND ----------

def retry[T](n: Int)(fn: => T): T = {
  util.Try { fn } match {
    case util.Success(x) => x
    case _ if n > 1 => retry(n - 1)(fn)
    case util.Failure(e) => {
      println("Error durante la ejecución del codigo luego de " + n + " intentos: " + e.getMessage()) 
      throw e
    }
  }
}

// COMMAND ----------

object PSQL {
  object Log extends Enumeration {
    type Log = Value
    val NULO, INFO, WARN, ERROR = Value
    // Log = 1,2,3
  }
  
  object Estatus extends Enumeration {
    type Estatus = Value
    val EXITOSO, DIFERENCIA, PERSISTEDIFERENCIA, ERROR = Value
    // Estatus = 1,2,3
  }
  
  def obtenerContadoresDB(jdbcUrl: String, schemaName : String, tableName : String, colName : String, startDate: Date, endDate: Date) : org.apache.spark.sql.DataFrame = {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrl)
    var retries = 1
    var contadores: Seq[Contador] = Seq[Contador]()

    val prepStatement = conn.prepareStatement(s"SELECT * FROM $schemaName.fn_obtener_contadores(?, ?, ?, ?, ?)")
    prepStatement.setString(1, schemaName)
    prepStatement.setString(2, tableName)
    prepStatement.setString(3, colName)
    prepStatement.setDate(4, startDate)
    prepStatement.setDate(5, endDate)
    try{
      retry(3){
        var rs  = prepStatement.executeQuery()
        while (rs.next()) {
          contadores = contadores :+ Contador(rs.getDate("fechaPresentacion"),
          rs.getLong("contador")
          )
        }
        retries = retries + 1
      }
      prepStatement.close();
      conn.close()
      contadores.toDF
    }
    catch{
      case e: Exception => 
        prepStatement.close();
        conn.close()
        throw e;
    }
  }
  
  def truncaTabla(jdbcUrl: String, nombretabla : String)= {
    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrl)
    var retries = 1
    
    val prepStatement = 
    conn.prepareStatement(s"""DELETE FROM ${nombretabla} WHERE id_proceso = ${ConfigurationProvider.identificadorIdProceso}""")
    // prepStatement.setString(1, esquema)
    // prepStatement.setString(2, nombretabla)

    try{
      retry(3){
        prepStatement.execute()
        retries = retries + 1
      }
      prepStatement.close();
    }
    catch{
      case e: Exception => 
        prepStatement.close();
        throw e;
    }
  }
  
  def getNullableString(valor: String = null) = {
    Option(valor).getOrElse("")
  }
}