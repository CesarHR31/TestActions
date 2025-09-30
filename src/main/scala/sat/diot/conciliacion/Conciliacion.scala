package sat.diot.conciliacion

import sat.diot.comunes.EnumsTablas.{listaTablasOro, listaTablasPlata}
import sat.diot.comunes.config.{ConfigurationProvider, EnumPlata}
import sat.diot.comunes.{CatalogoPasoEjecucionNPSI, EnumOro}
import sat.diot.conciliacion.UtilConciliacion._
import sat.diot.infraestructura.PostgresqlHandler
import sat.diot.ingesta.entities.RegistroMonitoreoNPSI
import sat.diot.parser.EjecutorScripts

import java.time.Duration
import java.time.temporal.ChronoUnit

object Conciliacion {

  private val startSep = "Array("
  private val endSep = ")"
  private val separator = ", "
  private lazy val db = new PostgresqlHandler(
    ConfigurationProvider.postgresqlControlUrl
  )
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  /**
   * Obtiene conteos de la tabla base y la tabla contra la que se realiza la conciliación
   *
   * @param nombreTablaAConciliar Tabla contra la que se realiza la conciliación.
   * @param nombreTablaBase       Tabla base para realizar la conciliación.
   * @param fechaInicio           Fecha inicial para obtener conteos.
   * @param fechaFin              Fecha final para obtener conteos.
   * @return
   */
  private def obtieneConteos(nombreTablaAConciliar: String, nombreTablaBase: String, fechaInicio: String, fechaFin: String): (ResultadoConteo, ResultadoConteo) = {
    //Obtener conteos de tabla base
    val insumosBase = obtenerConteosDeTabla(nombreTablaBase, fechaInicio, fechaFin)
    //Obtener conteos de tabla a conciliar
    val insumosAConciliar = obtenerConteosDeTabla(nombreTablaAConciliar, fechaInicio, fechaFin)

    (insumosBase, insumosAConciliar)
  }

  def procesaConciliacion(entidadProcesoConciliacion: EntidadProcesoConciliacion): ResultadoConciliaciones = {

    val inicioEjecucion = System.currentTimeMillis
    var diferenciaConteos: Long = 0L

    try {
      //Eliminamos duplicados en caso de existir.
      val resultadoDuplicados = eliminarDuplicados(entidadProcesoConciliacion.nombreTablaAConciliar, entidadProcesoConciliacion.fechaInicial, entidadProcesoConciliacion.fechaFinal)
      //Se obtienen conteos tanto de la tabla base como de la tabla a conciliar.
      val resultadoObtieneConteos: (ResultadoConteo, ResultadoConteo) = obtieneConteos(
        entidadProcesoConciliacion.nombreTablaAConciliar,
        entidadProcesoConciliacion.nombreTablaBase,
        entidadProcesoConciliacion.fechaInicial,
        entidadProcesoConciliacion.fechaFinal
      )
      diferenciaConteos = resultadoObtieneConteos._1.conteo - resultadoObtieneConteos._2.conteo //insumosBase.conteo - insumosAConciliar.conteo

      //Revisar conteos
      if (diferenciaConteos == 0) {
        //Conteos Iguales no se ejecuta la conciliación
        noEjecutaConciliacion(entidadProcesoConciliacion, resultadoDuplicados, diferenciaConteos, resultadoObtieneConteos)
      } else if (diferenciaConteos > 0) {
        //Se ejecuta la conciliación
        ejecutaConciliacion(entidadProcesoConciliacion, resultadoDuplicados, diferenciaConteos, resultadoObtieneConteos)
      } else {
        //Fallo catastrofico
        guardaRegistroConciliacion(RegistroConciliacion(entidadProcesoConciliacion.fechaInicial,
          entidadProcesoConciliacion.fechaFinal,
          entidadProcesoConciliacion.tipoConciliacion.toString,
          entidadProcesoConciliacion.nombreTablaAConciliar,
          resultadoDuplicados,
          resultadoObtieneConteos._1.conteo,
          resultadoObtieneConteos._2.conteo,
          diferenciaConteos,
          None,
          None,
          None,
          ProcesoConciliacion.FALLO_CATASTROFICO.toString,
          exitoso = false,
          "Existen menos registros en el origen que en la tabla a conciliar",
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMinutes
        ))

        throw new Exception(
          ProcesoConciliacion.FALLO_CATASTROFICO.toString + "|Existen menos registros en el origen que en la tabla a conciliar " + entidadProcesoConciliacion.nombreTablaAConciliar
        )
      }

    } catch {
      case e: Exception =>
        logger.error(s"Error al ejecutar conciliación de ${entidadProcesoConciliacion.tipoConciliacion.toString}: " + e.getMessage + "|" + e.getStackTrace
          .mkString(startSep, separator, endSep))
        println(s"Error al ejecutarconciliación de ${entidadProcesoConciliacion.tipoConciliacion.toString}: " + e.getMessage + "|" + e.getStackTrace
          .mkString(startSep, separator, endSep))

        var ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolPlata
        var idTabla = ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).idTabla
        if (entidadProcesoConciliacion.tipoConciliacion == TipoConciliacion.oro) {
          ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro
          idTabla = ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).idTabla
        }
        db.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
          entidadProcesoConciliacion.idEjecucion,
          ejecucionId,
          idTabla,
          if (entidadProcesoConciliacion.tipoConciliacion == TipoConciliacion.oro) ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name else ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name,
          0L,
          0L,
          exitoso = false,
          Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
          e.getMessage + "|" + e.getStackTrace.mkString(startSep, separator, endSep)
        ))

        ResultadoConciliaciones(
          entidadProcesoConciliacion.tipoConciliacion.toString,
          entidadProcesoConciliacion.nombreTablaAConciliar,
          diferenciaConteos,
          Left(e)
        )
    }
  }

  /**
   * No ejecuta conciliación en caso de que variable diferenciaConteos = 0
   *
   * @param procesoConciliacion objeto EntidadProcesoConciliacion.
   * @param resultadoDuplicados Total de registros duplicados en caso de existir.
   * @param diferenciaConteos   Registros diferentes en caso de existir.
   * @param resultadoConteos    Conteo de tabla base y tabla a conciliar.
   * @return
   */
  private def noEjecutaConciliacion(procesoConciliacion: EntidadProcesoConciliacion, resultadoDuplicados: Long, diferenciaConteos: Long, resultadoConteos: (ResultadoConteo, ResultadoConteo)): ResultadoConciliaciones = {
    val inicioEjecucion = System.currentTimeMillis
    //Conteos Iguales no se ejecuta la conciliación
    guardaRegistroConciliacion(RegistroConciliacion(procesoConciliacion.fechaInicial,
      procesoConciliacion.fechaFinal,
      procesoConciliacion.tipoConciliacion.toString,
      procesoConciliacion.nombreTablaAConciliar,
      resultadoDuplicados,
      resultadoConteos._1.conteo,
      resultadoConteos._2.conteo,
      diferenciaConteos,
      None,
      None,
      None,
      ProcesoConciliacion.CONTEOS_IGUALES.toString,
      exitoso = true,
      error = null,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMinutes))


    var ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolPlata
    var idTabla = ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).idTabla
    if (procesoConciliacion.tipoConciliacion == TipoConciliacion.oro) {
      ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro
      idTabla = ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).idTabla
    }
    db.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
      procesoConciliacion.idEjecucion,
      ejecucionId,
      idTabla,
      if (procesoConciliacion.tipoConciliacion == TipoConciliacion.oro) ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name else ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name,
      resultadoConteos._1.conteo,
      resultadoConteos._2.conteo,
      exitoso = true,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
      detalleError = null
    ))
    ResultadoConciliaciones(
      procesoConciliacion.tipoConciliacion.toString,
      procesoConciliacion.nombreTablaAConciliar,
      diferenciaConteos,
      Right(resultadoConteos._1.conteo, resultadoConteos._2.conteo)
    )
  }

  /**
   * Ejecuta conciliación en caso de que variable diferenciaConteos > 0
   *
   * @param procesoConciliacion objeto EntidadProcesoConciliacion.
   * @param resultadoDuplicados Total de registros duplicados en caso de existir.
   * @param diferenciaConteos   Registros diferentes en caso de existir.
   * @param resultadoConteos    Conteo de tabla base y tabla a conciliar.
   * @return
   */
  private def ejecutaConciliacion(procesoConciliacion: EntidadProcesoConciliacion, resultadoDuplicados: Long, diferenciaConteos: Long, resultadoConteos: (ResultadoConteo, ResultadoConteo)): ResultadoConciliaciones = {
    val inicioEjecucion = System.currentTimeMillis
    //Se ejecuta la conciliación
    //Se obtienen faltantes del origen
    val faltantes = obtenerFaltantesEntreTablas(resultadoConteos._1.insumos, resultadoConteos._2.insumos)

    var sampleFilesBasePath = ""

    if (procesoConciliacion.tipoConciliacion == TipoConciliacion.oro) {
      sampleFilesBasePath = ConfigurationProvider.pathScriptsLlenadoOro

      // Se borra en tabla a conciliar previo a inserción de faltantes
      listaTablasOro.foreach(tabla => {
        eliminarRegistrosDeTabla(
          tabla.name,
          procesoConciliacion.fechaInicial,
          procesoConciliacion.fechaFinal,
          faltantes
        )
      })

    } else if (procesoConciliacion.tipoConciliacion == TipoConciliacion.plata) {
      sampleFilesBasePath = ConfigurationProvider.pathScriptsLlenadoPlata

      // Se borra en tabla a conciliar previo a inserción de faltantes
      listaTablasPlata.foreach(tabla => {
        eliminarRegistrosDeTabla(
          tabla.name,
          procesoConciliacion.fechaInicial,
          procesoConciliacion.fechaFinal,
          faltantes
        )
      })
    }

    // re-ingresar
    if (faltantes != null && faltantes.nonEmpty) {
      new EjecutorScripts(sampleFilesBasePath)
        .procesarTablas("", faltantes, procesoConciliacion.fechaInicial, procesoConciliacion.fechaFinal, esReproceso = false)
    }
    //Se obtienen nuevamente los conteos despues de la conciliación
    val resultadoObtieneConteosFinal: (ResultadoConteo, ResultadoConteo) = obtieneConteos(
      procesoConciliacion.nombreTablaAConciliar,
      procesoConciliacion.nombreTablaBase,
      procesoConciliacion.fechaInicial,
      procesoConciliacion.fechaFinal
    )

    val diferenciaConteosFinal = resultadoObtieneConteosFinal._1.conteo - resultadoObtieneConteosFinal._2.conteo

    guardaRegistroConciliacion(RegistroConciliacion(procesoConciliacion.fechaInicial,
      procesoConciliacion.fechaFinal,
      procesoConciliacion.tipoConciliacion.toString,
      procesoConciliacion.nombreTablaAConciliar,
      resultadoDuplicados,
      resultadoConteos._1.conteo,
      resultadoConteos._2.conteo,
      diferenciaConteos,
      Some(resultadoObtieneConteosFinal._1.conteo),
      Some(resultadoObtieneConteosFinal._2.conteo),
      Some(diferenciaConteosFinal),
      ProcesoConciliacion.CONCILIACION_EJECUTADA.toString,
      exitoso = true,
      null,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMinutes)
    )

    var ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolPlata
    var idTabla = ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).idTabla
    if (procesoConciliacion.tipoConciliacion == TipoConciliacion.oro) {
      ejecucionId = CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro
      idTabla = ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).idTabla
    }
    db.registrarMonitoreoNPSI(RegistroMonitoreoNPSI(
      procesoConciliacion.idEjecucion,
      ejecucionId,
      idTabla,
      if (procesoConciliacion.tipoConciliacion == TipoConciliacion.oro) ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_decinfopeter).name else ConfigurationProvider.obtenerTablaPlataId(EnumPlata.identificadorTablaPlata_diot_decinfopeter).name,
      resultadoConteos._2.conteo,
      resultadoObtieneConteosFinal._2.conteo,
      exitoso = true,
      Duration.of(System.currentTimeMillis - inicioEjecucion, ChronoUnit.MILLIS).toMillis.toInt,
      null
    ))
    ResultadoConciliaciones(
      procesoConciliacion.tipoConciliacion.toString,
      procesoConciliacion.nombreTablaAConciliar,
      diferenciaConteos,
      Right(resultadoObtieneConteosFinal._1.conteo, resultadoObtieneConteosFinal._2.conteo)
    )
  }
}
