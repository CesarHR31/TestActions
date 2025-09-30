package sat.diot.validacion

import com.microsoft.applicationinsights.core.dependencies.apachecommons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite
import sat.diot.comunes.{EnumOro, SparkSessionManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import sat.diot.comunes.config.ConfigurationProvider

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.util.{Failure, Success, Try}

class Validacionjsonoro extends AnyFunSuite {
  System.setProperty("log4j2.configurationFile", s"${System.getProperty("user.dir")}\\src\\main\\resources\\log4j2config.xml")

  private val spark = SparkSessionManager.getOrCreateLocal
  private val UserDir = s"${System.getProperty("user.dir")}"

  import spark.implicits._

  test("Demo") {
    Seq(1, 2, 3).toDF().show()
    val valorEsperado = 1
    val valorObtenido = 1

    assertResult(valorEsperado) {
      valorEsperado
    }
    assert(valorEsperado.equals(valorObtenido))
  }

  test("mkstring test") {
    val startSep = "Array("
    val endSep = ")"
    val separator = ", "
    val seqTest = Seq(1, 2, 3)
    println(seqTest.mkString(startSep, separator, endSep))
  }

  test("Try/Success/Failure") {
    val valorTest = "CE"
    Try(Integer.parseInt(s"$valorTest")) match {
      case Success(value) => println(value)
      case Failure(ex) => println(ex.leftSide)
    }
  }

  test("Using rdd") {
    val df = Seq(1, 2, 4).toDF()
    val df1 = df.rdd.map { i =>
      i.getInt(0)
    }.toDF()

    df1.show()
  }

  test("Obtiene nombre de tablas a partir de scripts de llenado oro") {
    val scriptsLlenadoOro = s"$UserDir\\src\\main\\resources\\databricks\\scripts-llenado\\oro"
    val listPrefix = Seq("01", "vigente")
    new File(scriptsLlenadoOro)
      .listFiles()
      .filter(f => !listPrefix.exists(p => f.getName.toLowerCase.contains(p)))
      .map { file =>
        val nombreTabla = file.getName.replace(".sql", "").split("-")(1)
        println(nombreTabla)
      }
  }

  test("Obtiene nombre tabla") {
    val userDir = s"${System.getProperty(s"$UserDir")}"
    println(userDir)
    println(ConfigurationProvider.obtenerTablaOroId(EnumOro.identificadorTablaOro_infterdetiva).name)
  }

  test("Crea tabla local"){
    val tmpDir: File = Paths.get(s"$UserDir\\consultaTerceros").toFile
    val bdDirPath = tmpDir.getAbsolutePath.replace("\\", "\\\\")
    val database = "test_plata"
    println(bdDirPath)
    val directoryBd = s"$bdDirPath\\\\$database.bd"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION 'file:///$directoryBd'")
    spark.sql(s"USE $database")

    spark.sql(s"\nCREATE TABLE IF NOT EXISTS `$database`.`diot_oro_faltantes` (\n  `idejecucion` STRING,\n  `rfc` STRING,\n  `rfcdeclarante` STRING,\n  `numerooperacion` BIGINT,\n  `fechaIdentificacion` TIMESTAMP,\n  `estatus` SMALLINT)\nUSING delta;")

    spark.table(s"$database.diot_oro_faltantes").show(false)
  }

  test("Valida json vs Oro") {
    //Ruta donde se localiza la matriz de validación
    val rutaMatrizValidacion = s"$UserDir\\src\\test\\resources\\MatrizValidaciones.json"
    val esquemaOro = "diot_oro"
    //Ruta donde se encuentran los archivos parquet generados por el test de flujobase
    val rutaArchivosParquet = s"E:\\temp\\diot\\$esquemaOro"
    val escribeTablaValidaciones = "E:\\temp\\diot\\matrizvalidacion\\matrizvalidaciones"
    val rutaGeneraJson = "E:\\temp\\diot\\ResultadoValidaciones"

    //Definición de esquema para aplicarlo a la matriz de validación
    val structureSchema = new StructType()
      .add("tabla", StringType)
      .add("campo", StringType)
      .add("valorEsperado", StringType)
      .add("encontrado", StringType)

    //Lectura de matriz de validación agregando columna "encontrado"
    val dfMatriz = spark.read
      .format("json")
      .schema(structureSchema)
      .load(rutaMatrizValidacion)
      .withColumn("encontrado", lit("NO"))

    dfMatriz
      .write
      .format("delta")
      .mode("overwrite")
      .save(s"$escribeTablaValidaciones")

    val tablaDeltaMatriz = spark.read
      .format("delta")
      .load(s"$escribeTablaValidaciones")

    tablaDeltaMatriz.createOrReplaceTempView("matrizdeltaview")

    new File(rutaArchivosParquet)
      .listFiles()
      .foreach { f =>
        val nombreTabla = f.toString.split("\\\\")(4)
        println(s"=====> Validando campos de la tabla: $nombreTabla")

        //Lectura tabla por tabla en oro
        val dfOro = spark.read
          .format("delta")
          .load(s"$f")
          .filter($"numerooperacion" === "227945685719")

        val listaColumnas = dfOro.columns
        listaColumnas.foreach { column =>
          //Se cambia el Formato de la Columna a String
          val dfColumns = dfOro.withColumn(column + "_temp", dfOro.col(column).cast(StringType))
            .drop(column)
            .withColumnRenamed(column + "_temp", column)

          //Se obtienen los datos encontrados en la Tabla|Columna
          val datosColumna = dfColumns.select(column).rdd.map(r => r(0)).collect()

          //Se filtran las filas de la matriz por Tabla|Columna
          val filtroTablaCampo = dfMatriz.filter(s"tabla == '$nombreTabla' and campo == '$column'")

          //Obtiene valor esperado para la Tabla|columna
          val valorCampoBuscado = filtroTablaCampo.rdd.map(row => row.getString(2)).collect()
          if (valorCampoBuscado.length > 0) {
            //Se recorren los valores esperados de la matriz
            valorCampoBuscado.map { valorEsperado =>
              //Se recorren valores obtenidos de la tabla
              datosColumna.map { valorObtenido =>
                val random = new scala.util.Random
                val indiceCaso = random.nextInt(100)
                //Si el valorEsperado es igual al valorObtenido, actualiza el campo "encontrado" a 'SI'
                if (valorEsperado.equals(valorObtenido)) {
                  spark.sql(s"UPDATE matrizdeltaview SET encontrado = 'SI' WHERE tabla = '$nombreTabla' AND campo = '$column' AND valorEsperado = '$valorObtenido'")
                  println(s"Caso con ID: $indiceCaso ====> Tabla: $nombreTabla | Campo: $column | Valor esperado: $valorEsperado | Valor obtenido: $valorObtenido")
                }
              }
            }
          }
        }
        println("Valores no encontrados en tablas oro:")
        spark.sql("SELECT * FROM matrizdeltaview WHERE encontrado = 'NO'").show()

        println("Resultado de las validaciones en: E:\\temp\\diot\\20241015\\ResultadoValidaciones")
        spark.sql("SELECT * FROM matrizdeltaview")
          .write
          .mode("overwrite")
          .json(rutaGeneraJson)

        moveFile(rutaGeneraJson, "E:\\temp\\diot")

        FileUtils.deleteDirectory(new File(rutaGeneraJson))

      }
  }

  def moveFile(sourcePath: String, targetPath: String): Unit = {
    new File(sourcePath)
      .listFiles()
      .map { file =>
        if (file.getName.startsWith("part-00000")) {
          println(file.getName)
          Files.move(Paths.get(s"$sourcePath\\${file.getName}"), Paths.get(s"$targetPath\\resultado_validacion.json"), StandardCopyOption.REPLACE_EXISTING)
        }
      }
  }
}
