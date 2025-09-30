package sat.diot.ingesta.entities

import org.apache.spark.sql.types._

import java.sql.Timestamp

object EsquemasCapas {

  val schemaLand = StructType(
    List(
      StructField("FechaCarga", StringType, true),
      StructField("id_ejecucion", IntegerType, true),
      StructField("Rfc", StringType, true),
      StructField("NumeroOperacion", LongType, true),
      StructField("Obligaciones", StringType, true),
      StructField("FechaDeclaracion", TimestampType, true),
      StructField("Ejercicio", IntegerType, true)
    )
  )

  case class Land(
      fechacarga: String,
      id_ejecucion: String,
      rfc: String,
      numerooperacion: Long,
      obligaciones: String,
      fechadeclaracion: Timestamp,
      ejercicio: Int
  )

  case class IdEstatus(
      idestatus: Int,
      descripcionestatus: String
  )

  case class UltimaVigenteLand(
                                partitionkey: String,
                                rowkey: String,
                                timestamp: Timestamp,
                                obligaciones: String,
                                concepto: String,
                                rfc: String,
                                numerooperacion: Long,
                                fechadeclaracion: Timestamp,
                                ejercicio: Int,
                                periodicidad: String,
                                periodo: String,
                                tipodeclaracion: String,
                                tipocomplementaria: String,
                                estatusdeclaracion: Int,
                                identificadordeclaracion: String,
                                identificadordeclaracionpadre: String,
                                identificadordeclaracionraiz: String,
                                idejecucion: String
                              )

}
