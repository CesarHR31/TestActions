package sat.diot.reproceso.core

import sat.diot.ingesta.core.Excepciones.LandException
import sat.diot.ingesta.entities.EsquemasCapas.Land
import sat.diot.reproceso.utils.AzureBlobs
import slick.util.Logging

import java.sql.Timestamp

object ProcesamientoDatos extends Logging {

  def creacionLand(listado: List[String], idEjecucion: String): List[Land] = {

    try {
      logger.info("Iniciada creacion de modelo Land")
      val resultadoLand = listado.map(a => {
        Land(
          "Reproceso",
          idEjecucion,
          a.split("/").last.split("\\.").head,      //rfc
          a.split("/").last.split("\\.")(1).toLong, //numOperacion
          "",
          new Timestamp(System.currentTimeMillis()),
          0
        )
      })
      logger.info("Modelo land creado correctamente")
      resultadoLand
    } catch {
      case e: Exception =>
        logger.error(s"Ocurrio un error al crear Land, error", e)
        throw LandException(e.getMessage, e)
    }
  }

  def recuperarListado(storageClient: AzureBlobs, nombreBlob: String): List[String] = {

    val blobRef = storageClient.getBlobReference(nombreBlob)

    val textoListado = storageClient.getBlobText(blobRef)
    textoListado.split("\\r").flatMap(_.split("\\n")).filter(_.nonEmpty).toList
  }

}
