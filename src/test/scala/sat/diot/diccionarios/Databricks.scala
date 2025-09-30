package sat.diot.diccionarios

import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}
import sat.diot.comunes._

import java.io._

class Databricks extends UnitSpecWithDatabase {
  test("Genera diccionario BD") {
    val listaBases = List("diot_ctl", "dec_inf_diotl_land", "dec_inf_diotl_bronce", "dec_inf_diotl_plata", "dec_inf_diotl")

    listaBases.foreach(dbName => {
      val workbook = new XSSFWorkbook()

      spark.catalog.listTables(dbName).collect().foreach { table =>
        val tableFields = SparkUtils.getSchemaInTabularFormat(table.name, dbName).collect()

        val sheet = workbook.createSheet(table.name)

        val estiloCabecera = UtileriasDocumentacion.generarEstiloCabecera(sheet)

        var row = sheet.createRow(0)
        var cell = row.createCell(0)
        cell.setCellValue("Campo")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(1)
        cell.setCellValue("Tipo Dato")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(2)
        cell.setCellValue("Acepta Nulos")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(3)
        cell.setCellValue("Descripción")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(4)
        cell.setCellValue("Clave SAT")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(5)
        cell.setCellValue("Es Partition")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(6)
        cell.setCellValue("Esquema")
        cell.setCellStyle(estiloCabecera)

        cell = row.createCell(7)
        cell.setCellValue("Tabla")
        cell.setCellStyle(estiloCabecera)

        var rowCounter = 1

        tableFields.foreach { row =>
          val innerRow = sheet.createRow(rowCounter)

          var cell = innerRow.createCell(0)
          cell.setCellValue(row.getString(0))

          cell = innerRow.createCell(1)
          cell.setCellValue(row.getString(1).replace("Type",""))

          cell = innerRow.createCell(2)
          cell.setCellValue(row.getBoolean(2))

          cell = innerRow.createCell(3)
          cell.setCellValue(row.getString(3))

          cell = innerRow.createCell(4)
          cell.setCellValue(row.getString(4))

          cell = innerRow.createCell(5)
          cell.setCellValue(row.getBoolean(5))

          cell = innerRow.createCell(6)
          cell.setCellValue(dbName)

          cell = innerRow.createCell(7)
          cell.setCellValue(table.name)

          rowCounter += 1
        }

        (0 to 7).foreach {
          sheet.autoSizeColumn
        }

        val destinationPath = s"E:\\temp\\Diccionarios\\diccionario-${dbName}.xlsx"

        new java.io.File(destinationPath) {
          getParentFile.mkdirs()
          createNewFile()
        }

        val fileStream = new FileOutputStream(destinationPath)
        workbook.write(fileStream)
        fileStream.flush()
        fileStream.close()
      }

    })
  }
}

object UtileriasDocumentacion {

  def generarEstiloCabecera(sheet: XSSFSheet): CellStyle = {
    val font = sheet.getWorkbook.createFont
    font.setBold(true)
    val style = sheet.getWorkbook.createCellStyle()
    style.setFont(font)
    style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex)
    style.setFillPattern(FillPatternType.SOLID_FOREGROUND)
    style
  }
}
