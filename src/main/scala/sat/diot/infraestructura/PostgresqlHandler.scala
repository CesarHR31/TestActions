package sat.diot.infraestructura

import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.comunes.{CONSTANTS, CatalogoPasoEjecucionNPSI, CatalogoProcesoNPSI, Util}
import sat.diot.infraestructura.tablas._
import sat.diot.ingesta.core.Excepciones.PostgresWriteException
import sat.diot.ingesta.entities.{CatalogoProceso, RegistroMonitoreoNPSI}
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

class PostgresqlHandler(jdbcUrl: String) extends Serializable {
  lazy val slickDatabase = Database.forURL(jdbcUrl)
  private lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  val cuentas = TableQuery[PostgresAzureAccountTables]
  val tablaCifrasControl = TableQuery[CifrasControlTables]
  val tablaHistorico = TableQuery[HistoricoTables]
  val tablaMonitoreoNPSI = TableQuery[MonitoreoGeneralNPSIControlTables]
  val tablaControlGzip = TableQuery[GzipControlTables]
  val tablaSecretsWatDeclaracionConceptoFecha = TableQuery[SecretsWatDeclaracionConceptoFechaTables]
  val tablaBitacoraProcesoDatos = TableQuery[BitacoraProcesoDatosTable]
  val tablaBitacoraDetallePaso = TableQuery[BitacoraDetallePasoTable]
  val tablaBitacoraReprocesos = TableQuery[BitacoraReprocesosTable]
  val tablaWATDeclaracionAnualTable = TableQuery[ConfiguracionWATDeclaracionAnualTable]

  var timeout: FiniteDuration = Duration(5, TimeUnit.MINUTES)

  def actualizarEstatusBatch(idBatch: String, estatus: Int): Unit = {

    logger.info(s"Actualizando estatus de batch $idBatch a $estatus")

    val query = tablaCifrasControl
      .filter(_.id_ejecucion === idBatch)
      .map(_.idEstatus)
      .update(estatus)

    logger.info(s"Actualizado estatus de batch $idBatch a $estatus")
    Await.result(slickDatabase.run(query), timeout)
  }

  def actualizarEstatusReproceso(idBatch: String, estatus: Int, idProceso: Int): Unit = {

    val query = tablaBitacoraReprocesos
      .filter(_.idejecucion === idBatch)
      .filter(_.id_proceso === idProceso)
      .map(_.estatus)
      .update(estatus)

    Await.result(slickDatabase.run(query), timeout)
  }

  def obtenerMetadataBatch(idBatch: String): Option[CifrasControlTable] = {
    val query = tablaCifrasControl.filter(_.id_ejecucion === idBatch).take(1)

    Await
      .result(slickDatabase.run(query.result), timeout)
      .headOption
  }

  def registrarNuevoBatch(batchMetadata: CifrasControlTable): Unit = {
    val insertAction = tablaCifrasControl ++= Seq(batchMetadata)

    Await.result(slickDatabase.run(insertAction), timeout)
  }

  def obtenerBatchConEstatus(estatus: Int): Seq[CifrasControlTable] = {
    val query = tablaCifrasControl.filter(_.idEstatus === estatus)

    Await.result(slickDatabase.run(query.result), timeout)
  }

  def obtenerCuentasWAT(): Seq[SecretsWatDeclaracionConceptoFechaTable] = {
    val query = tablaSecretsWatDeclaracionConceptoFecha.filter(_.vigente)

    Await.result(slickDatabase.run(query.result), timeout)
  }

  def obtenerConfiguracionWAT(): Seq[ConfiguracionWATDeclaracionAnual] = {
    val query = tablaWATDeclaracionAnualTable.filter(_.vigente)

    Await.result(slickDatabase.run(query.result), timeout)
  }

  def obtenerSasDisponibles(): Seq[PostgresAzureAccountTable] = {
    Await
      .result(slickDatabase.run(cuentas.result), Duration.Inf)
  }

  def actualizaCifrasControlBronce(id: String, bronce: Long, bronceValidos: Long, bronceError: Long): Unit = {
    val updateQuery = tablaCifrasControl
      .filter(_.id_ejecucion === id)
      .map { r => (r.cifrasBronce, r.cifrasBronceValidos, r.cifrasBronceError) }

    val updateAction = updateQuery
      .update(
        (
          bronce,
          bronceValidos,
          bronceError
        )
      )

    logger.debug(updateQuery.updateStatement)

    Await.result(slickDatabase.run(updateAction), Duration.Inf)
  }

  def actualizaCifrasControlPlata(id: String, plata: Long): Unit = {
    val query = tablaCifrasControl
      .filter(_.id_ejecucion === id)
      .map(_.cifrasPlata)
      .update(plata)

    Await.result(slickDatabase.run(query), timeout)
  }

  def actualizaCifrasControlOro(id: String, oro: Long): Unit = {
    val query = tablaCifrasControl
      .filter(_.id_ejecucion === id)
      .map(_.cifrasOro)
      .update(oro)

    Await.result(slickDatabase.run(query), timeout)
  }

  def insertaHistorico(idEjecucion: String): Unit = {

    try {
      val entidadHistorico = HistoricoTable(
        idEjecucion,
        Util.obtenerTimestamp()
      )

      val insertAction = tablaHistorico ++= Seq(entidadHistorico)

      insertAction.statements.foreach { s => logger.debug(s) }

      val result = Await
        .result(slickDatabase.run(insertAction), Duration.Inf)

      logger.info(result.toString)
      logger.info(s"Insertado Id de proceso $idEjecucion en la base de datos de cifras")
    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error al insertar en Postgres el id en la tabla historica", e)
        throw PostgresWriteException(e.getMessage, e)
    }
  }

  def insertaCifrasControl(idEjecucion: String): Unit = {

    try {
      val entidadCifrasControl = CifrasControlTable(
        idEjecucion,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        Util.obtenerTimestamp(),
        CatalogoProceso.PROCESANDOLAND,
        Util.obtenerTimestamp(),
        null,
        esReproceso = false,
        null,
        null,
        null
      )

      val insertAction = tablaCifrasControl ++= Seq(entidadCifrasControl)

      insertAction.statements.foreach { s => logger.debug(s) }

      val result = Await
        .result(slickDatabase.run(insertAction), Duration.Inf)

      logger.info(result.toString)
      logger.info(s"Insertado lote con Id de proceso $idEjecucion en la base de datos de cifras")
    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error al insertar en Postgres las cifras de control", e)
        throw PostgresWriteException(e.getMessage, e)
    }
  }

  def actualizaCifrasControlLand(cifrasControlTable: CifrasControlTable): Unit = {
    val updateQuery = tablaCifrasControl
      .filter(_.id_ejecucion === cifrasControlTable.id_ejecucion)
      .map { r => (r.cifrasRecepcion, r.idEstatus, r.fechaActualizacion) }

    val updateAction = updateQuery
      .update(
        (
          cifrasControlTable.cifrasRecepcion,
          cifrasControlTable.idEstatus,
          cifrasControlTable.fechaActualizacion
        )
      )

    logger.debug(updateQuery.updateStatement)

    Await.result(slickDatabase.run(updateAction), Duration.Inf)
  }

  def actualizaEstadoBatch(cifrasControlTable: CifrasControlTable): Unit = {
    val updateQuery = tablaCifrasControl
      .filter(_.id_ejecucion === cifrasControlTable.id_ejecucion)
      .map { r => (r.idEstatus, r.fechaActualizacion) }

    val updateAction = updateQuery
      .update(cifrasControlTable.idEstatus, cifrasControlTable.fechaActualizacion)

    logger.debug(updateQuery.updateStatement)

    Await.result(slickDatabase.run(updateAction), Duration.Inf)
  }

  def actualizaIdEstatusBatch(idEjecucion: String, estatus: Int): Unit = {
    val updateQuery = tablaCifrasControl
      .filter(_.id_ejecucion === idEjecucion)
      .map { r => (r.idEstatus, r.fechaActualizacion) }

    val updateAction = updateQuery
      .update(estatus, Util.obtenerTimestamp())

    logger.debug(updateQuery.updateStatement)

    Await.result(slickDatabase.run(updateAction), Duration.Inf)
  }


  def registrarMonitoreoNPSI(registroMonitoreoNPSI: RegistroMonitoreoNPSI): Unit = {

    val error = org.apache.commons.lang3.StringUtils.left(registroMonitoreoNPSI.detalleError, 5000)
    if (!registroMonitoreoNPSI.esCuarentena) {
      val insertAction = tablaMonitoreoNPSI ++= Seq(
        MonitoreoGeneralNPSIControlTable(
          registroMonitoreoNPSI.idEjecucion,
          CatalogoProcesoNPSI.ProcesoDesdobleInformacion,
          registroMonitoreoNPSI.idPasoEjecucion,
          registroMonitoreoNPSI.idTabla,
          registroMonitoreoNPSI.registrosOrigen,
          registroMonitoreoNPSI.registrosProceso,
          registroMonitoreoNPSI.exitoso,
          registroMonitoreoNPSI.tiempoDeEjecucion,
          if (error == null) {
            ""
          } else {
            error
          },
          Util.obtenerTimestamp()
        )
      )
      Await.result(slickDatabase.run(insertAction), timeout)
    }

    // Se discriminan los pasos de conciliación
    if (registroMonitoreoNPSI.idPasoEjecucion != CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolOro && registroMonitoreoNPSI.idPasoEjecucion != CatalogoPasoEjecucionNPSI.ConciliacioncifrascontrolPlata) {
      registrarDetallePaso(registroMonitoreoNPSI.idEjecucion, registroMonitoreoNPSI.idPasoEjecucion, registroMonitoreoNPSI.nombreTabla.toUpperCase, registroMonitoreoNPSI.registrosProceso, registroMonitoreoNPSI.esCuarentena)
    }
  }

  private def registrarDetallePaso(
                                    idEjecucion: String,
                                    idPasoEjecucion: Int,
                                    nombreTabla: String,
                                    contadorRegistros: Long,
                                    esCuarentena: Boolean = false
                                  ): Unit = {

    val insertActionMon = tablaBitacoraDetallePaso ++= Seq(
      BitacoraDetallePaso(
        idEjecucion,
        CONSTANTS.BITACORAS_ID_PROCESO,
        idPasoEjecucion match {
          case 15 => CONSTANTS.BITACORAS_PASO_RECEPCION
          case 16 => CONSTANTS.BITACORAS_PASO_DESDOBLE
          case 17 => CONSTANTS.BITACORAS_PASO_PLATA
          case 19 => CONSTANTS.BITACORAS_PASO_ORO
          case _ => idPasoEjecucion
        },
        "Inserción en " + (idPasoEjecucion match {
          case 16 =>
            if (esCuarentena)
              "cuarentena"
            else
              "recepción exitosa"

          case _ => nombreTabla
        }),
        idPasoEjecucion match {
          case 16 =>
            if (esCuarentena)
              "BRONCE/CUARENTENA"
            else
              "BRONCE/RECEPCIÓN"

          case _ => nombreTabla
        }, //Objeto
        contadorRegistros.toInt,
        Util.obtenerTimestamp()
      )
    )
    Await.result(slickDatabase.run(insertActionMon), timeout)
  }

  def registrarControlGzip(
                            idEjecucion: String,
                            nombretabla: String,
                            idTablaNpsi: Int,
                            rutagzip: String
                          ): Unit = {

    val insertAction = tablaControlGzip ++= Seq(
      GzipControlTable(
        idEjecucion,
        nombretabla,
        idTablaNpsi,
        rutagzip,
        CatalogoProceso.PROCESANDO_ENVIO_GZIPS,
        null,
        Util.obtenerTimestamp()
      )
    )

    Await.result(slickDatabase.run(insertAction), timeout)
  }

  def actualizaEstatusProcesoDatos(idEjecucion: String, idPasoEjecucion: Int, idProceso: Int, estatus: Int, detalleError: String): Unit = {
    val updateQuery = tablaBitacoraProcesoDatos
      .filter(_.idEjecucion === idEjecucion)
      .filter(_.idPasoEjecucion === idPasoEjecucion)
      .filter(_.idProceso === idProceso)
      .map { r => (r.estatus, r.detalleError) }

    val updateAction = updateQuery
      .update((estatus, Some(detalleError)))

    logger.debug(updateQuery.updateStatement)

    Await.result(slickDatabase.run(updateAction), Duration.Inf)
  }

  def insertaBitacoraProcesoDatos(data: BitacoraProcesoDatos): Unit = {
    val i = tablaBitacoraProcesoDatos ++= Seq(data)
    Await.result(slickDatabase.run(i), timeout)
  }

}
