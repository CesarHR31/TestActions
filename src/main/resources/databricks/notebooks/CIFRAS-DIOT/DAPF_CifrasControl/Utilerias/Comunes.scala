// Databricks notebook source
import java.sql.Date
import java.time.{ZoneId, ZonedDateTime, LocalDate}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, when, date_format, abs, datediff, coalesce, to_timestamp, substring_index}
import io.delta.tables._
import scala.math.ceil

// COMMAND ----------

case class Contador (fechaPresentacion: Date, contadornpsi: Long)
case class tablasAContar (esquemaNombre: String, tablaNombe: String, columnaNombre: String)

// COMMAND ----------

def appendText(textToAppend: String, sequenceToAppend: Seq[String], order: Boolean = true): Seq[String] = {
  if (order) {
    sequenceToAppend.map(x => textToAppend + x)
  }
  else {
    sequenceToAppend.map(x => x + textToAppend)
  }
}

def keysToCondition(keys: Seq[String]): String = {
  keys.map(x => s"source.$x = updates.$x").mkString(" and ")
}

implicit class RegexOps(sc: StringContext) {
  def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
}

// COMMAND ----------

object Escritura {
  def escribirDelta(df: DataFrame, nombretabla: String, modoEscritura: String, opcionEsquemaClave: String = "", opcionEsquemaValor: String = "", particion: String = ""): Unit = {
    df
    .write
    .format("delta")
    .mode(modoEscritura)    
    .saveAsTable(nombretabla)
  }

  def actualizaDelta(df: DataFrame, nombretabla: String, llaves: Seq[String]): Unit = {
    val sourceTable = spark.table(nombretabla)
    val sourceColumns = sourceTable.columns.filterNot(r => llaves.contains(r))
    val dfColumns = df.columns.filterNot(r => llaves.contains(r))
    val newColumns = appendText("updates.", dfColumns)
    val mapeoColumnasUpdate = (sourceColumns zip newColumns).toMap
    val mapeoColumnasInsert = (sourceColumns ++ llaves zip newColumns ++ appendText("updates.", llaves)).toMap

    val deltaTable = DeltaTable.forName(nombretabla)

    deltaTable.as("source")
    .merge(df.as("updates"), keysToCondition(llaves))
    .whenMatched
    .updateExpr(
      mapeoColumnasUpdate
    )
    .whenNotMatched
    .insertExpr(
      mapeoColumnasInsert
    )
    .execute()
  }
}

// COMMAND ----------

object Comunes
{
  def timeIt[T](op: => T): (T, Long) = {
    val start  = System.currentTimeMillis
    val result = op
    val end    = System.currentTimeMillis
    (result, end - start)
  }

  def convertMilisecondToPart(ms: Long, part: String) : Long = {
    val tiempo:Long = part match {
      case "horas" => ceil(ms/3600000).toLong
      case "minutos" => ceil(ms/60000).toLong
      case "segundos" => ceil(ms/1000).toLong
      case "miliSegundos" => ms
      case _ => throw new IllegalArgumentException(s"$part no es una unidad valida")
    }
    
    tiempo
  }
  
  def convertMilisecondToString(ms: Long) : String = {
    val hora = ms/3600000;
    var msRestantes = ms%3600000;
    val minutos = msRestantes/60000;
    msRestantes = msRestantes%60000;
    val segundos = msRestantes/1000;
    val miliSegundos = msRestantes%1000;
    
    "%02d:%02d:%02d.%d".format(hora, minutos, segundos, miliSegundos)
  }
  
  def getFileContents(path: String): String = {
    val scalaSource = scala.io.Source
      .fromFile(path)

    val fileContents = scalaSource.mkString

    scalaSource.close()

    fileContents
  }
}