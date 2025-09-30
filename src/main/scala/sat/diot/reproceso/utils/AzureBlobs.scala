package sat.diot.reproceso.utils

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer, CloudBlockBlob, ListBlobItem}

class AzureBlobs(connString: String, containerName: String) extends Serializable {

  private val csa: CloudStorageAccount    = CloudStorageAccount.parse(connString)
  private val blobClient: CloudBlobClient = csa.createCloudBlobClient()
  private val containerReference: CloudBlobContainer =
    blobClient.getContainerReference(containerName)

  def getBlobReference(nombreBlob: String): CloudBlockBlob = {
    containerReference.getBlockBlobReference(nombreBlob)
  }

  def getBlobText(cloudBlockBlob: CloudBlockBlob): String = {

    cloudBlockBlob.downloadText()

  }

  def listBlobs(): java.lang.Iterable[ListBlobItem] = {
    val blobList = containerReference.listBlobs()
    blobList
  }

}
