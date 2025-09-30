package sat.diot.parser

import com.azure.storage.blob.{BlobContainerClient, BlobContainerClientBuilder}
import com.azure.storage.blob.models.{BlobErrorCode, BlobStorageException}
import com.azure.storage.blob.specialized.BlobInputStream
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.Logger
import sat.diot.comunes.config.ConfigurationProvider

import scala.util.{Failure, Success, Try}

class BlobContainerManager(nombreBlob: String, cuenta: String, sas: String) extends Serializable {
  lazy val logger: Logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  private def obtenerClienteParaCuenta: Option[BlobContainerClient] = {
    try {
      val blobContainerClient: BlobContainerClient = new BlobContainerClientBuilder()
        .endpoint(cuenta)
        .sasToken(sas)
        .containerName(cuenta.split("/").last)
        .buildClient()

      Some(blobContainerClient)

    } catch {
      case _: NoSuchElementException => None
      case e: Exception => throw e;
    }
  }

  def obtenerInputStreamParaBlobEnCuenta: Option[BlobInputStream] = {
    try {
      Some(
        obtenerClienteParaCuenta match {
          case Some(client) =>
            client
              .getBlobClient(nombreBlob)
              .getBlockBlobClient
              .openInputStream()
          case None =>
            val errMsg = s"No existe cliente para la cuenta $cuenta. Revisar inicialización de parser."
            logger.error(errMsg)
            throw new Exception(errMsg)
        }
      )
    } catch {
      case e: BlobStorageException =>
        (e.getErrorCode, e.getStatusCode) match {
          case (BlobErrorCode.BLOB_NOT_FOUND, 404) => throw e
        }
    }
  }

  def getBlobSize: Either[Throwable, Long] = {
    Try(obtenerInputStreamParaBlobEnCuenta) match {
      case Success(value) => Right(value.get.getProperties.getBlobSize / FileUtils.ONE_MB)
      case Failure(e) =>
        val message = s"Blob $nombreBlob no encontrado en cuenta $cuenta"
        logger.error(message)
        Left(e)
    }
  }
}
