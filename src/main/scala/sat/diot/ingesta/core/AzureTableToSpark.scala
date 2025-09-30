package sat.diot.ingesta.core

import com.microsoft.azure.storage.table.TableServiceEntity
import sat.diot.ingesta.utils.AzureTable

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.reflect.ClassTag

abstract class AzureTableToSpark[A <: TableServiceEntity, B](implicit c: ClassTag[A]) {

  val tableName: String
  val azureTable = new AzureTable(getConnectionString, tableName)

  def getConnectionString: String

  def serviceEntityToCaseClass(serviceEntity: A): B

  def writeWithSpark(rows: List[B]): Unit

  def downloadRows(query: String): Seq[B] = {
    azureTable
      .executeQuery[A](query)
      .asScala
      .toList
      .map(a => serviceEntityToCaseClass(a))
  }

}
