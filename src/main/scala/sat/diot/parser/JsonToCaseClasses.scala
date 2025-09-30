package sat.diot.parser

import com.google.gson.{Gson, GsonBuilder, JsonObject}
import org.jsfr.json.provider.GsonProvider
import org.jsfr.json.{GsonParser, JsonPathListener, JsonSurfer, ParsingContext}
import sat.diot.comunes.config.ConfigurationProvider
import sat.diot.parser.estandarizacion._

import java.io.InputStream
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.util
import java.util.Collections
import scala.collection.mutable.ListBuffer
import scala.util.Try

class JsonToCaseClasses(clavesSAT: Map[String, String]) {

  val errorList: util.List[ErrorParser] =
    Collections.synchronizedList(new util.ArrayList[ErrorParser]())

  private val gson: Gson = new GsonBuilder()
    .registerTypeAdapter(classOf[Timestamp], new TimestampCustomAdapter(errorList, clavesSAT))
    .registerTypeAdapter(classOf[Date], new DateCustomAdapter(errorList, clavesSAT))
    .registerTypeAdapter(classOf[Integer], new IntCustomAdapter(errorList, clavesSAT))
    .registerTypeAdapter(classOf[JBigDecimal], new DecimalCustomAdapter(errorList, clavesSAT))
    .create

  private val jsonSurfer = new JsonSurfer(GsonParser.INSTANCE, new GsonProvider(gson))

  def processSmallJson(json: String): Either[Throwable, EsquemaDiot] = {
    val r = Try(gson.fromJson(json, classOf[EsquemaDiot])).toEither

    if (r.isLeft) {
      errorList.add(ErrorParser(null, null, null, r.left.get.getMessage))
    }

    r
  }

  def obtenerErrores: util.List[ErrorParser] = errorList

  def processBigJson(inputStream: InputStream, inputSurferStream: InputStream): Seq[EsquemaDiot] = {
    val headerJson: (IdentificacionDeLaDeclaracion, Contribuyente, Declaracion) = getHeaderJsonAndCloseStream(inputStream)
    val bodySurferJson: AdminDeclaracion = getBodyJsonWithStreamSurfer(inputSurferStream)
    val groupedList: ListBuffer[EsquemaDiot] = new ListBuffer[EsquemaDiot]()

    bodySurferJson.DatosDelTerceroDeclarado
      .grouped(ConfigurationProvider.agrupacionSurferJson)
      .foreach { group =>
        groupedList += EsquemaDiot(headerJson._1, headerJson._2, headerJson._3, Array(bodySurferJson.copy(DatosDelTerceroDeclarado = group)))
      }

    groupedList
  }

  private def getHeaderJsonAndCloseStream(inputStream: InputStream): (IdentificacionDeLaDeclaracion, Contribuyente, Declaracion) = {
    val jsonCollector = jsonSurfer.collector(inputStream)
    val identiDecla = jsonCollector.collectOne("$.IdentiDecla", classOf[IdentificacionDeLaDeclaracion])
    val contribuyente = jsonCollector.collectOne("$.Contribuyente", classOf[Contribuyente])
    val declaracion = jsonCollector.collectOne("$.Declaracion", classOf[Declaracion])
    jsonCollector.exec()
    inputStream.close()

    (identiDecla.get(), contribuyente.get(), declaracion.get())
  }

  private def getBodyJsonWithStreamSurfer(stream: InputStream): AdminDeclaracion = {
    val listBufferTerceroDeclarado = ListBuffer[DatosDelTerceroDeclarado]()
    val listBufferDetDatosInformativos = ListBuffer[DetDatosInformativos]()
    var totales: Totales = null

    jsonSurfer
      .configBuilder()
      .bind(
        "$.AdminDeclaracion[0].DatosDelTerceroDeclarado[*]",
        new JsonPathListener {
          override def onValue(value: Any, context: ParsingContext): Unit = {
            val jsonObject = value.asInstanceOf[JsonObject]
            val parsedJson = gson.fromJson(jsonObject, classOf[DatosDelTerceroDeclarado])
            listBufferTerceroDeclarado += parsedJson
          }
        }
      )
      .bind(
        "$.AdminDeclaracion[0].DetDatosInformativos[*]",
        new JsonPathListener {
          override def onValue(value: Any, context: ParsingContext): Unit = {
            val jsonObject = value.asInstanceOf[JsonObject]
            val parsedJson = gson.fromJson(jsonObject, classOf[DetDatosInformativos])
            listBufferDetDatosInformativos += parsedJson
          }
        }
      )
      .bind(
        "$.AdminDeclaracion[0].Totales",
        new JsonPathListener {
          override def onValue(value: Any, context: ParsingContext): Unit = {
            val jsonObject = value.asInstanceOf[JsonObject]
            totales = gson.fromJson(jsonObject, classOf[Totales])
          }
        }
      )
      .buildAndSurf(stream)


    stream.close()

    AdminDeclaracion(
      listBufferTerceroDeclarado.toArray,
      totales,
      listBufferDetDatosInformativos.toArray
    )
  }
}
