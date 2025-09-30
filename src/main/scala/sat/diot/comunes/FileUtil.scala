package sat.diot.comunes

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.FileUtils
import sat.diot.comunes.config.ConfigurationProvider

import java.io._

object FileUtil {
  lazy val logger = org.apache.logging.log4j.LogManager.getLogger(ConfigurationProvider.nombreProyecto)

  def fileContainsBOM(path: String): Boolean = {
    val bom         = Array.fill[Byte](3)(0)
    val inputStream = new FileInputStream(path)
    inputStream.read(bom)
    val stringContent = new String(Hex.encodeHex(bom))
    val containsBOM   = stringContent.equalsIgnoreCase("efbbbf")
    inputStream.close()
    containsBOM
  }

  def addBomToFile(path: String): Unit = {
    logger.info(s"Adding BOM to file at $path")
    val lineIterator   = FileUtils.lineIterator(new File(path), "UTF-8")
    val tempFile       = s"$path.tmp"
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8"))
    bufferedWriter.write(0xfeff)

    try {
      logger.info(s"Copying rows from $path to $tempFile")
      while (lineIterator.hasNext) {
        val str = lineIterator.nextLine()
        bufferedWriter.write(str + "\r\n")
      }

      lineIterator.close()
      bufferedWriter.close()
      logger.info(s"Deleting source file and renaming temp file to source name.")
      FileUtils.deleteQuietly(new File(path))
      FileUtils.moveFile(new File(tempFile), new File(path))
    } finally {
      lineIterator.close()
      bufferedWriter.close()
    }
  }
}
