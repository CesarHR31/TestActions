package sat.diot.ingesta.utils

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.TableQuery.{Operators, QueryComparisons}
import com.microsoft.azure.storage.table._
import sat.diot.ingesta.entities.{WATDeclaracion, WATDescarga}

import scala.reflect.ClassTag

class AzureTable(connString: String, tableName: String) extends Serializable {

  private val csa: CloudStorageAccount      = CloudStorageAccount.parse(connString)
  private val tableClient: CloudTableClient = csa.createCloudTableClient()
  private val tableReference: CloudTable    = tableClient.getTableReference(tableName)
  private val StartQuery: String = "(PartitionKey ge '"

  def insert(entity: TableServiceEntity): Unit = {
    val insertOp = TableOperation.insert(entity)
    tableReference.execute(insertOp)
  }

  def insertOrReplace(entity: TableServiceEntity): Unit = {
    val insertOrReplaceOp = TableOperation.insertOrReplace(entity)
    tableReference.execute(insertOrReplaceOp)
  }

  def insertOrMerge(entity: TableServiceEntity): Unit = {
    val insertOrReplaceOp = TableOperation.insertOrMerge(entity)
    tableReference.execute(insertOrReplaceOp)
  }

  def retrieve(partitionKey: String, rowKey: String, classType: TableServiceEntity): TableResult = {
    val retrieveOp = TableOperation.retrieve(partitionKey, rowKey, classType.getClass)
    tableReference.execute(retrieveOp)
  }

  def queryParam(filtroI: String, filtroF: String): java.lang.Iterable[WATDescarga] = {
    val queryOp = this.executeQuery[WATDescarga](StartQuery + filtroI + "') and (PartitionKey lt '" + filtroF + "')")
    queryOp
  }

  def queryParam(): java.lang.Iterable[WATDescarga] = {
    val queryOp = this.executeQuery[WATDescarga]("PartitionKey ge ''")
    queryOp
  }

  def queryParam(filtroI: String, filtroF: String, filtroED: Int): java.lang.Iterable[WATDescarga] = {
    val queryOp = this.executeQuery[WATDescarga](StartQuery + filtroI + "') and (PartitionKey lt '" + filtroF + "')  and (Ejercicio ge " + filtroED + ")")
    queryOp
  }

  def queryDeclaracionAnual(query: String): java.lang.Iterable[WATDeclaracion] = {
    val queryOp = this.executeQuery[WATDeclaracion](query)
    queryOp
  }

  def queryParamCifras(filtro: String, ejercicio: String, tipoPersona:String): java.lang.Iterable[WATDescarga] = {
    val queryOp = this.executeQuery[WATDescarga](StartQuery + filtro + "000000') and (PartitionKey le '" + filtro + "235999') and (Ejercicio ge " + ejercicio + ") and (TipoPersona eq '"+ tipoPersona+"')")
    queryOp
  }

  def queryParamCifras(filtro: String, ejercicio: String): java.lang.Iterable[WATDescarga] = {
    val queryOp = this.executeQuery[WATDescarga](StartQuery + filtro + "000000') and (PartitionKey le '" + filtro + "235999') and (Ejercicio ge " + ejercicio + ")")
    queryOp
  }

  /*TEST*/
  def getDeclaracionConcepto(obligaciones: String): java.lang.Iterable[WATDeclaracion] = {
    val queryOp = this.executeQuery[WATDeclaracion](s"Obligaciones eq '$obligaciones'")
    queryOp
  }

  def executeQuery[T <: TableEntity](filters: String)(implicit tag: ClassTag[T]): java.lang.Iterable[T] = {
    val queryOp = TableQuery.from(tag.runtimeClass.asInstanceOf[Class[T]]).where(filters)
    tableReference.execute(queryOp)
  }

  def delete(entity: TableServiceEntity): Unit = {
    val deleteOp = TableOperation.delete(entity)
    tableReference.execute(deleteOp)
  }

  def replace(entity: TableServiceEntity): Unit = {
    val replaceOp = TableOperation.replace(entity)
    tableReference.execute(replaceOp)
  }

  def deleteTableIfExists(): Unit = {
    tableReference.deleteIfExists()
  }

  def createTableIfNotExists(): Unit = {
    tableReference.createIfNotExists()
  }

  def exists: Boolean = {
    tableReference.exists()
  }

  def retrieveRange[WATDescarga <: TableEntity](iRange: String, fRange: String)(implicit
    tag: ClassTag[WATDescarga]
  ): java.lang.Iterable[WATDescarga] = {
    val iPartitionFilter = TableQuery.generateFilterCondition("PartitionKey", QueryComparisons.GREATER_THAN_OR_EQUAL, iRange)
    val fPartitionFilter = TableQuery.generateFilterCondition("PartitionKey", QueryComparisons.LESS_THAN, fRange)
    val combinedFilter   = TableQuery.combineFilters(iPartitionFilter, Operators.AND, fPartitionFilter)

    val rangeQuery =
      TableQuery
        .from(tag.runtimeClass.asInstanceOf[Class[WATDescarga]])
        .where(combinedFilter)

    tableReference.execute(rangeQuery)
  }
}
