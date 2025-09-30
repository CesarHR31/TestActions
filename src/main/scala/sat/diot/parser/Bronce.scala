package sat.diot.parser

import java.sql.Timestamp

case class Bronce(
                   numeroOperacion: Long,
                   rfc: String,
                   fechaPresentacion: Timestamp,
                   id_ejecucion: String,
                   blobpath: String,
                   declaracionValida: Boolean,
                   timestampProcesamiento: Timestamp,
                   errores: Array[ErrorParser],
                   payload: Option[EsquemaDiot]
                 )
