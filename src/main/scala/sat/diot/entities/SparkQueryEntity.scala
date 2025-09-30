package sat.diot.entities

import sat.diot.cifrascontrol.TipoConteoCifras.TipoConteoCifras

/**
 * Entidad que encapsula los parámetros para ejecutar la consulta de cifras.
 *
 * @param sourceTableName Nombre de la tabla fuente sobre la que se realiza el conteo.
 * @param initDate Fecha de inicio del rango de datos.
 * @param endDate Fecha de fin del rango de datos.
 * @param executionId Identificador único de la ejecución.
 * @param targetTableName Nombre de la tabla destino donde se registran los resultados.
 * @param typeCountingFigures Enumerador de la capa (bronce, plata u oro).
 * @param idTable Identificador lógico de la tabla fuente
 * @param executionStepId Identificador de la etapa en la que se ejecuta el conteo.
 */
case class SparkQueryEntity(sourceTableName: String,
                            initDate: String,
                            endDate: String,
                            executionId: String,
                            targetTableName: String,
                            typeCountingFigures: TipoConteoCifras,
                            idTable: Int,
                            executionStepId: Int
                           )
