package sat.diot.parser

import java.sql.Timestamp

case class MetadataValidaSize(
                               fechacarga: String,
                               id_ejecucion: String,
                               rfc: String,
                               numerooperacion: String,
                               obligaciones: String,
                               fechadeclaracion: Timestamp,
                               ejercicio: String,
                               blobpath: String,
                               size: Long,
                               tipoFlujo: Integer
                             )
