package sat.diot.diccionarios

import org.apache.poi.ss.usermodel.{CellStyle, FillPatternType, IndexedColors}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._

import java.io.FileOutputStream
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Psql extends sat.diot.comunes.DockerJDBCSpec with sat.diot.comunes.PostgresAndScripts {
  test("Generar diccionario psql") {
    val db = Database.forURL(getJdbcUrl)

    val query =
      sql"""SELECT c.table_schema,c.table_name,c.column_name, c.is_nullable, c.data_type,pgd.description
              FROM pg_catalog.pg_statio_all_tables AS st
              INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
              INNER JOIN information_schema.columns c ON (pgd.objsubid=c.ordinal_position AND
                                c.table_schema=st.schemaname AND c.table_name=st.relname);
            """.as[(String, String, String, String, String, String)]


    val results = Await.result(db.run(query), Duration.Inf)

    val workbook = new XSSFWorkbook()

    val sheet = workbook.createSheet("postgres")

    val styles = generarEstiloCabecera(sheet)

    val row = sheet.createRow(0)
    var cell = row.createCell(0)
    cell.setCellValue("Esquema")
    cell.setCellStyle(styles)

    cell = row.createCell(1)
    cell.setCellValue("Tabla")
    cell.setCellStyle(styles)

    cell = row.createCell(2)
    cell.setCellValue("Campo")
    cell.setCellStyle(styles)

    cell = row.createCell(3)
    cell.setCellValue("Acepta Nulos")
    cell.setCellStyle(styles)

    cell = row.createCell(4)
    cell.setCellValue("Tipo Dato")
    cell.setCellStyle(styles)

    cell = row.createCell(5)
    cell.setCellValue("Descripción")
    cell.setCellStyle(styles)

    var rowCounter = 1

    results.foreach { row =>
      val innerRow = sheet.createRow(rowCounter)

      var cell = innerRow.createCell(0)
      cell.setCellValue(row._1)

      cell = innerRow.createCell(1)
      cell.setCellValue(row._2)

      cell = innerRow.createCell(2)
      cell.setCellValue(row._3)

      cell = innerRow.createCell(3)
      cell.setCellValue(row._4)

      cell = innerRow.createCell(4)
      cell.setCellValue(row._5)

      cell = innerRow.createCell(5)
      cell.setCellValue(row._6)

      rowCounter += 1
    }

    (0 to 3).foreach {
      sheet.autoSizeColumn
    }

    val destinationPath = s"E:\\temp\\Diccionarios\\diccionario-single_diot.xlsx"

    new java.io.File(destinationPath) {
      getParentFile.mkdirs()
      createNewFile()
    }

    val fileStream = new FileOutputStream(destinationPath)
    workbook.write(fileStream)
    fileStream.flush()
    fileStream.close()


    println(query)
  }

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
