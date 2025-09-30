package sat.diot.parser

import java.sql.Timestamp

case class MetadataIngesta(
    fechacarga: String,
    id_ejecucion: String,
    rfc: String,
    numerooperacion: String,
    obligaciones: String,
    fechadeclaracion: Timestamp,
    ejercicio: String,
    blobpath: String
)
