package sat.diot.conciliacion

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.{EnumControl, SparkSessionManager, Util}

import java.sql.Timestamp

object UtilConciliacion {
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  def eliminarDuplicados(
                          nombreTabla: String,
                          fechaInicial: String,
                          fechaFinal: String
                        ): Long = {

    val nombreTablaDelta = "default.diot_tabladeltaconciliacion_h"

    try {
      if (spark.catalog.tableExists(nombreTablaDelta)) {
        spark.sql(s"DROP TABLE $nombreTablaDelta")
      }
    } catch {
      case e: Exception => null
    }

    // Se obtiene delta de información a traves de partición
    spark
      .sql(s"""SELECT *
                            FROM (
                                  SELECT *
                                  FROM $nombreTabla
                                  WHERE p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                                        AND date_trunc('MM','$fechaFinal')
                                 ) T1
                            WHERE T1.fechapresentacion BETWEEN '$fechaInicial' AND '$fechaFinal'
                            """)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(nombreTablaDelta)

    logger.info(s"Revisando si existen duplicados en $nombreTabla")
    val ExistenDuplicados = spark.sql(s"""
                                            SELECT numerooperacion FROM $nombreTablaDelta
                                            GROUP BY numerooperacion
                                            HAVING COUNT(*)>1
                                        """)

    if (ExistenDuplicados.count > 0) {
      logger.info(s"Eliminando duplicados en: $nombreTabla")

      val duplicadosYClones =
        spark.sql(s"""
                                SELECT *
                                FROM $nombreTablaDelta A
                                WHERE fechapresentacion >= '$fechaInicial' AND
                                      fechapresentacion < '$fechaFinal' AND
                                       EXISTS (
                                               SELECT *
                                               FROM (
                                                     SELECT *, row_number() OVER (PARTITION BY numerooperacion ORDER BY fechadesdoble ASC) secuencial
                                                     FROM $nombreTablaDelta
                                                     WHERE fechapresentacion >= '$fechaInicial' AND
                                                           fechapresentacion < '$fechaFinal'
                                                    ) tmp
                                               WHERE tmp.secuencial > 1 AND tmp.fechapresentacion = A.fechapresentacion
                                                     AND tmp.numerooperacion = A.numerooperacion and tmp.fechadesdoble = A.fechadesdoble
                                              )
                                """)

      val nombreTablaTemporal = nombreTabla + "_tmp"

      try {
        if (spark.catalog.tableExists(nombreTablaTemporal))
          spark.sql(s"DROP TABLE $nombreTablaTemporal")
      } catch {
        case e: Exception => null
      }

      duplicadosYClones.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nombreTablaTemporal)

      val listaDuplicados = spark
        .table(nombreTablaTemporal)
        .select(col("numerooperacion"))
        .distinct()
        .collect
        .map(_.getAs[Long](0))
        .toList

      spark.sql(s"""
                              DELETE FROM $nombreTabla A
                              WHERE A.p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                                    AND date_trunc('MM','$fechaFinal')
                                    AND A.numerooperacion IN(${listaDuplicados
        .mkString(",")})
                            """)

      val clonesFiltradosParaReinsertar = spark
        .table(nombreTablaTemporal)
        .withColumn(
          "rn",
          row_number.over(
            Window
              .partitionBy(
                col("numerooperacion"),
                col("fechapresentacion"),
                col("fechadesdoble")
              )
              .orderBy(asc("fechadesdoble"))
          )
        )
        .filter(col("rn") === 2)
        .drop(col("rn"))

      clonesFiltradosParaReinsertar.write.format("delta").mode("append").saveAsTable(nombreTabla)

      spark.sql(s"DROP TABLE $nombreTablaTemporal")

      ExistenDuplicados.count
    } else {
      logger.info(s"No se encontraron duplicados en: $nombreTabla")
      0L
    }
  }

  def obtenerConteosDeTabla(
                             nombreTabla: String,
                             fechaInicial: String,
                             fechaFinal: String
                           ): ResultadoConteo = {

    logger.info(s"Obteniendo conteos de tabla: $nombreTabla")
    val insumos = spark.sql(s"""
                                        SELECT T1.numerooperacion
                                        FROM (
                                              SELECT * FROM $nombreTabla
                                              WHERE p_fechapresentacion BETWEEN date_trunc('MM','$fechaInicial')
                                                    AND date_trunc('MM','$fechaFinal')
                                             ) T1
                                        WHERE T1.fechapresentacion >= '$fechaInicial'
                                              AND T1.fechapresentacion < '$fechaFinal'
                                        """)

    ResultadoConteo(insumos.count(), insumos)
  }

  def eliminarRegistrosDeTabla(
                                nombreTabla: String,
                                fechaInicial: String,
                                fechaFinal: String,
                                listaNumerosOperacion: List[Long]
                              ): Unit = {

    if (listaNumerosOperacion != null && listaNumerosOperacion.nonEmpty) {
      logger.info(s"Eliminando registros de tabla: $nombreTabla")

      spark.sql(s"""
                            DELETE FROM $nombreTabla A
                                   WHERE A.p_fechapresentacion BETWEEN date_trunc('MM', '$fechaInicial')
                                         AND date_trunc('MM','$fechaFinal')
                                         AND A.numerooperacion IN(${listaNumerosOperacion.mkString(
        ","
      )})
                            """)
    }
  }

  def spark: SparkSession = SparkSessionManager.session

  def obtenerFaltantesEntreTablas(
                                   insumoOrigen: DataFrame,
                                   insumoDestino: DataFrame
                                 ): List[Long] = {

    logger.info(s"Obteniendo faltantes")
    insumoOrigen
      .as("A")
      .join(
        insumoDestino.as("B"),
        col("A.numerooperacion") === col("B.numerooperacion"),
        "left_anti"
      )
      .select(col("A.numerooperacion"))
      .collect
      .map(_.getLong(0))
      .toList
  }

  def guardaRegistroConciliacion(entidadRegistroConciliacion: RegistroConciliacion): Unit = {
    val spark2 = spark
    import spark2.implicits._
    
    val bitacoras = Seq(
      RegistroBitacora(
        Timestamp.valueOf(entidadRegistroConciliacion.fechainicial),
        Timestamp.valueOf(entidadRegistroConciliacion.fechafinal),
        entidadRegistroConciliacion.modeloaconciliar: String,
        entidadRegistroConciliacion.tablaaconciliar: String,
        entidadRegistroConciliacion.duplicados,
        entidadRegistroConciliacion.conteoinicialesperado,
        entidadRegistroConciliacion.conteoinicialactual,
        entidadRegistroConciliacion.diferenciainicial,
        entidadRegistroConciliacion.conteofinalesperado,
        entidadRegistroConciliacion.conteofinalactual,
        entidadRegistroConciliacion.diferenciafinal,
        entidadRegistroConciliacion.procesorealizado,
        entidadRegistroConciliacion.exitoso,
        entidadRegistroConciliacion.error,
        entidadRegistroConciliacion.tiempoenminutostardoenprocesar,
        Util.obtenerTimestamp()
      )
    )

    bitacoras.toDF.write
      .format("delta")
      .mode("append")
      .saveAsTable(ConfigurationProvider.obtenerTablaControlId(EnumControl.identificadorTablaConciliacion).name)
  }
}
