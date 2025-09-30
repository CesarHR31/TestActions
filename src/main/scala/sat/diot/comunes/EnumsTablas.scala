package sat.diot.comunes

import sat.diot.comunes.config.ConfigurationProvider.configs
import sat.diot.comunes.config.SparkTable

object EnumsTablas {

  val listaTablasOro: List[SparkTable] =
    configs.databases
      .flatMap { db =>
        db.tables.map {
            a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
          }
          .filter(_.name.contains(EnumDataBase.dec_inf_diotl.toString))
          .filter(!_.name.contains("land"))
          .filter(!_.name.contains("bronce"))
          .filter(!_.name.contains("plata"))
          .filter(!_.name.contains("ultimadeclaracion"))
          .filter(!_.name.toUpperCase.contains("VISTA"))
      }.toList

  val listaTablasPlata: List[SparkTable] =
    configs.databases
      .flatMap { db =>
        db.tables.map {
            a => SparkTable(s"${db.name}.${a.name}", a.id, a.idTabla)
          }.filter(_.name.contains(EnumDataBase.dec_inf_diotl_plata.toString))
          .filter(!_.name.contains("ultimadeclaracion"))
          .filter(!_.name.toUpperCase.contains("VISTA"))
      }.toList
}
