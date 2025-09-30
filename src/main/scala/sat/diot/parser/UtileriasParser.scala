package sat.diot.parser

object UtileriasParser {
  def limpiarPath(path: String): String = {
    val regex = "\\[\\d+\\]"
    path
      .replaceAll(regex, "")
      .replace("$.", "")
  }
}
