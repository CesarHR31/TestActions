package sat.diot.reproceso.entities

import java.util.Date

case class WATDescargaBlob(
    var FechaCarga: String,
    var IdentificadorDeclaracion: String,
    var Rfc: String,
    var NumeroOperacion: Long,
    var Obligaciones: String,
    var FechaDeclaracion: Date,
    var Ejercicio: Int
)
