package sat.diot.flujobase.sizeFile

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp

class FileSize extends AnyFunSuite{

  test("Get File Size"){
    val pathFile: String = "E:\\Pictures\\Imagen.HEIC"
    val file: File = new File(pathFile)

    if(file.exists() && file.isFile){
      val fileSize: Double = file.length().toDouble / FileUtils.ONE_MB
      println(s"El tamaño del archivo es de $fileSize MB.")
    }
    else
      println("El archivo especificado no existe o no es un archivo.")
  }

  test ("Compare dates"){
    val fi = Timestamp.valueOf("2024-12-03 12:00:00")
    val ff = Timestamp.valueOf("2024-12-03 18:41:07")

    if(ff.compareTo(fi) > 0)
      println("La fecha final es mayor a la inicial")
    else
      println("La fecha inicial es mayor a la final")
  }
}
