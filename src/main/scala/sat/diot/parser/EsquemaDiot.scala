package sat.diot.parser

import java.lang.{Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.Timestamp


/**
 * DIOT
 *
 * @param IdentiDecla      Datos de identificación de la declaración.
 * @param Contribuyente    Información del contribuyente.
 * @param Declaracion      Información de la Declaracion.
 * @param AdminDeclaracion Administración de la Declaración.
 */
case class EsquemaDiot(IdentiDecla: IdentificacionDeLaDeclaracion,
                       Contribuyente: Contribuyente,
                       Declaracion: Declaracion,
                       AdminDeclaracion: Array[AdminDeclaracion]
                      )

/**
 * Datos de identificación de la declaración.
 *
 * @param rfcdeclarante     Registro federal del contribuyente
 * @param verformu          Versión del formulario.
 * @param fechapresentacion Fecha de presentación de la declaración.
 * @param ejercicio         Ejericio de la declaración.
 * @param numerooperacion   Número de operación.
 */
case class IdentificacionDeLaDeclaracion(rfcdeclarante: String,
                                         verformu: String,
                                         fechapresentacion: Timestamp,
                                         ejercicio: JShort,
                                         numerooperacion: JLong)

/**
 * Información del contribuyente.
 *
 * @param bo_id                Identificador del contribuyente
 * @param tipopersona          Tipo de persona
 * @param razonsocial          Denominación o razón social
 * @param razonsocialhistorica Denominación o razón social histórica
 * @param apellidopaterno      Apellido paterno
 * @param apellidomaterno      Apellido materno
 * @param nombre               Nombre (s)
 * @param iddesconcentrada     Desconcentrada
 * @param desdesconcentrada    Descripción desconcentrada
 * @param obligaciones         Obligaciones
 * @param fechaingresoapp      Fecha de ingreso a la aplicación
 */
case class Contribuyente(bo_id: String,
                         tipopersona: String,
                         razonsocial: String,
                         razonsocialhistorica: String,
                         apellidopaterno: String,
                         apellidomaterno: String,
                         nombre: String,
                         iddesconcentrada: String,
                         desdesconcentrada: String,
                         obligaciones: JShort,
                         fechaingresoapp: Timestamp)

/**
 * Información de la Declaracion.
 *
 * @param periodo               Identificación del periodo.
 * @param peridiocidad          Peridiocidad
 * @param tipodeclara           Tipo de declaración
 * @param tipocomp              Tipo de complementaria
 * @param vencimientoobligacion Vencimiento de la obligación
 * @param mediopresentacion     Medio de presentación
 * @param alsc                  ALSC donde presentó la declaración
 * @param idtiposoc             Cve tipo sociedad
 * @param desctiposoc           Descripción tipo de sociedad
 * @param periodicidadDesc      Descripción de la periodicidad
 * @param periododesc           Descripción del periodo
 * @param tipoDeclaDesc         Descripción del tipo de declaración
 * @param tipoComplementIdDesc  Descripción del tipo de complementaria
 */
case class Declaracion(periodo: String,
                       peridiocidad: String,
                       tipodeclara: String,
                       tipocomp: String,
                       vencimientoobligacion: Timestamp,
                       mediopresentacion: JBigDecimal,
                       alsc: String,
                       idtiposoc: String,
                       desctiposoc: String,
                       periodicidadDesc: String,
                       periododesc: String,
                       tipoDeclaDesc: String,
                       tipoComplementIdDesc: String)

/**
 * @param DatosDelTerceroDeclarado Datos del tercero declarado.
 * @param Totales                  Totales.
 * @param DetDatosInformativos     Detalle Datos Informativos (Estimulos Fiscales)
 */
case class AdminDeclaracion(DatosDelTerceroDeclarado: Array[DatosDelTerceroDeclarado],
                            Totales: Totales,
                            DetDatosInformativos: Array[DetDatosInformativos]
                           )

/**
 *
 * @param fila                              Consecutivo del elemento
 * @param numeroconsecutivoperacion         Número consecutivo de operación
 * @param tipotercero                       Tipo de tercero
 * @param tipoperacion                      Tipo de operación
 * @param rfctercerodeclarado               Registro federal de contribuyentes del tercero declarado
 * @param numeroidfiscal                    Número de identificación fiscal
 * @param nombreextranjero                  Nombre del extranjero
 * @param paisjurisdiccionresidenciafiscal  País o jurisdicción de residencia fiscal
 * @param especificalugarjurisdiccionfiscal Especificar lugar de jurisdicción fiscal
 */
case class DatosDelTerceroDeclarado(fila: JLong,
                                    numeroconsecutivoperacion: JLong,
                                    tipotercero: String,
                                    tipoperacion: String,
                                    rfctercerodeclarado: String,
                                    numeroidfiscal: String,
                                    nombreextranjero: String,
                                    paisjurisdiccionresidenciafiscal: String,
                                    especificalugarjurisdiccionfiscal: String,
                                    IvaDeclararPorTerceroProveedor: IvaDeclararPorTerceroProveedor,
                                    ValorActActi: ValorActActi,
                                    IvaAcreditable: IvaAcreditable,
                                    IvaNoAcreditable: IvaNoAcreditable,
                                    DatosAdicionales: DatosAdicionales
                                   )

/**
 * IVA a declarar por tercero o proveedor.
 *
 * @param valactactipagtasa16iva                     Valor de los actos o actividades pagados a la tasa del 16% de IVA
 * @param ivapagatasa16                              IVA pagado a la tasa del 16%
 * @param montoivapagadonoacreditasa16               Monto del IVA pagado no acreditable a la tasa del 16%
 * @param valactactipagatasa11iva                    Valor de los actos o actividades pagados a la tasa del 11% de IVA
 * @param ivapagadotasa11                            IVA pagado a la tasa del 11%
 * @param montoivapagadonoacreditasa11               Monto del IVA pagado no acreditable a la tasa del 11%
 * @param valactactipagarfn                          Valor de los actos o actividades pagados en la región fronteriza norte
 * @param ivapagarfn                                 IVA pagado en la región fronteriza norte
 * @param montoivapagadonoacredirfn                  Monto del IVA pagado no acreditable en la región fronteriza norte
 * @param valactactipagarfs                          Valor de los actos o actividades pagados en la región fronteriza sur
 * @param ivapagadorfs                               IVA pagado en la región fronteriza sur
 * @param montoivapagadonoacredirfs                  Monto del IVA pagado no acreditable en la región fronteriza sur
 * @param valactactipagaimpbienservitasa16iva        Valor de los actos o actividades pagados en la importación de bienes y servicios a la tasa del 16% de IVA
 * @param ivapagaimpbienservitasa16iva               IVA pagado en la importación de bienes y servicios a la tasa del 16%
 * @param montoivapaganoacreimporbienservtasa16iva   Monto del IVA pagado no acreditable en la importación de bienes y servicios a la tasa del 16% de IVA
 * @param valactactipagaimpobienservinopagaivaexento Valor de los actos o actividades pagados en la importación de bienes y servicios por los que no se pagará el IVA (Exentos)
 * @param valactactipaganopagaivaexe                 Valor de los actos o actividades pagados por los que no se pagará el IVA (Exentos)
 * @param valactactipagatasa0iva                     Valor de los actos o actividades pagados a la tasa del 0% de IVA
 * @param valactactinoobjiva                         Valor de los actos o actividades no objeto del IVA
 * @param ivapagaretenido                            IVA pagado (retenido)
 * @param ivapagadevdesbonsobadqmercgastgener        IVA pagado por devoluciones, descuentos y bonificaciones sobre adquisición de mercancías y gastos en general
 */
case class IvaDeclararPorTerceroProveedor(valactactipagtasa16iva: JBigDecimal,
                                          ivapagatasa16: JBigDecimal,
                                          montoivapagadonoacreditasa16: JBigDecimal,
                                          valactactipagatasa11iva: JBigDecimal,
                                          ivapagadotasa11: JBigDecimal,
                                          montoivapagadonoacreditasa11: JBigDecimal,
                                          valactactipagarfn: JBigDecimal,
                                          ivapagarfn: JBigDecimal,
                                          montoivapagadonoacredirfn: JBigDecimal,
                                          valactactipagarfs: JBigDecimal,
                                          ivapagadorfs: JBigDecimal,
                                          montoivapagadonoacredirfs: JBigDecimal,
                                          valactactipagaimpbienservitasa16iva: JBigDecimal,
                                          ivapagaimpbienservitasa16iva: JBigDecimal,
                                          montoivapaganoacreimporbienservtasa16iva: JBigDecimal,
                                          valactactipagaimpobienservinopagaivaexento: JBigDecimal,
                                          valactactipaganopagaivaexe: JBigDecimal,
                                          valactactipagatasa0iva: JBigDecimal,
                                          valactactinoobjiva: JBigDecimal,
                                          ivapagaretenido: JBigDecimal,
                                          ivapagadevdesbonsobadqmercgastgener: JBigDecimal)

/**
 * Valor de actos o actividades.
 *
 * @param valtotactactipagarfn                          Valor total de actos o actividades pagadas en la región fronteriza norte
 * @param devdesbonrfn                                  Devoluciones, descuentos y bonificaciones en la región fronteriza norte
 * @param valnetactactipagarfn                          Valor neto de actos o actividades pagados en la región fronteriza norte
 * @param ivapagadorfndet                               IVA pagado en la región fronteriza norte
 * @param valtotactactipagarfs                          Valor total de actos o actividades pagadas en la región fronteriza sur
 * @param devdesbonrfs                                  Devoluciones, descuentos y bonificaciones en la región fronteriza sur
 * @param valnetactactipagarfs                          Valor neto de actos o actividades pagados en la región fronteriza sur
 * @param ivapagadorfsdet                               IVA pagado en la región fronteriza sur
 * @param valtotactactipagarf                           Valor total de actos o actividades pagadas en la región fronteriza
 * @param devdesbonrf                                   Devoluciones, descuentos y bonificaciones en la región fronteriza
 * @param valnetactactipagarf                           Valor neto de actos o actividades pagados en la región fronteriza
 * @param ivapagarf                                     IVA pagado en la región fronteriza
 * @param valtotactactipagatasa16iva                    Valor total de actos o actividades pagadas a la tasa del 16% de IVA
 * @param devdesbontasa16iva                            Devoluciones, descuentos y bonificaciones a la tasa del 16% de IVA
 * @param valnetactactipagatasa16iva                    Valor neto de actos o actividades pagadas a la tasa del 16% de IVA
 * @param ivapagatasa16det                              IVA pagado a la tasa del 16%
 * @param ivapagaagresinincimpor                        IVA pagado agregado sin incluir importaciones
 * @param valtotactactipagaimporaduabientangtasa16iva   Valor total de actos o actividades pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param devdesbonimporaduabientangtasa16iva           Devoluciones, descuentos y bonificaciones en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param valnetactactiimporaduabientangtasa16iva       Valor neto de actos o actividades en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param ivapagaimporaduabientangtasa16                IVA pagado en la importación por aduana de bienes tangibles a la tasa del 16%
 * @param valtotactactipagaimporbienintangservtasa16iva Valor total de actos o actividades pagadas en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param devdesbonimporbienintangservtasa16iva         Devoluciones, descuentos y bonificaciones en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param valnetactactiimporbienintangservtasa16iva     Valor neto de actos o actividades en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param ivapagaimporbienintangservtasa16              IVA pagado en la importación de bienes intangibles y servicios a la tasa del 16%
 * @param ivapagsumimpvalagreincluimpor                 IVA pagado Suma del impuesto al valor agregado incluyendo importaciones
 */
case class ValorActActi(valtotactactipagarfn: JBigDecimal,
                        devdesbonrfn: JBigDecimal,
                        valnetactactipagarfn: JBigDecimal,
                        ivapagadorfndet: JBigDecimal,
                        valtotactactipagarfs: JBigDecimal,
                        devdesbonrfs: JBigDecimal,
                        valnetactactipagarfs: JBigDecimal,
                        ivapagadorfsdet: JBigDecimal,
                        valtotactactipagarf: JBigDecimal,
                        devdesbonrf: JBigDecimal,
                        valnetactactipagarf: JBigDecimal,
                        ivapagarf: JBigDecimal,
                        valtotactactipagatasa16iva: JBigDecimal,
                        devdesbontasa16iva: JBigDecimal,
                        valnetactactipagatasa16iva: JBigDecimal,
                        ivapagatasa16det: JBigDecimal,
                        ivapagaagresinincimpor: JBigDecimal,
                        valtotactactipagaimporaduabientangtasa16iva: JBigDecimal,
                        devdesbonimporaduabientangtasa16iva: JBigDecimal,
                        valnetactactiimporaduabientangtasa16iva: JBigDecimal,
                        ivapagaimporaduabientangtasa16: JBigDecimal,
                        valtotactactipagaimporbienintangservtasa16iva: JBigDecimal,
                        devdesbonimporbienintangservtasa16iva: JBigDecimal,
                        valnetactactiimporbienintangservtasa16iva: JBigDecimal,
                        ivapagaimporbienintangservtasa16: JBigDecimal,
                        ivapagsumimpvalagreincluimpor: JBigDecimal
                       )


/**
 * IVA Acreditable.
 *
 * @param excluactgravrfn                                   Exclusivamente de actividades gravadas en la región fronteriza norte
 * @param asoacticualaplproprfn                             Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza norte
 * @param ivaacrerfn                                        IVA acreditable en la región fronteriza norte
 * @param excluactgravrfs                                   Exclusivamente de actividades gravadas en la región fronteriza sur
 * @param asoactcualaplproprfs                              Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza sur
 * @param ivaacrerfs                                        IVA acreditable en la región fronteriza sur
 * @param excluactgravrf                                    Exclusivamente de actividades gravadas en la región fronteriza
 * @param asoactcualaplproprf                               Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza
 * @param ivaacrerf                                         IVA acreditable en la región fronteriza
 * @param excluactgravpagatasa16iva                         Exclusivamente de actividades gravadas pagados a la tasa del 16% de IVA
 * @param asoacticualapliproppagatasa16iva                  Asociado a actividades por las cuales se aplicó una proporción pagados a la tasa del 16% de IVA
 * @param ivaacrepagatasa16                                 IVA acreditable pagado a la tasa del 16%
 * @param excluactigravsininclimpor                         Exclusivamente de actividades gravadas sin incluir importaciones
 * @param asoacticualaplipropsininclimpor                   Asociado a actividades por las cuales se aplicó una proporción sin incluir importaciones
 * @param ivaacresininclimpor                               IVA acreditable sin incluir importaciones
 * @param excluactigravpagaimporaduabientangtasa16iva       Exclusivamente de actividades gravadas pagadas en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoacticualapliproppagaimporaduabientangtasa16iva Asociado a actividades por las cuales se aplicó una proporción pagadas en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param ivaacrepagaimpoaduabientangtasa16iva              IVA acreditable pagado en la importación por aduana de bienes tangibles a la tasa del 16%
 * @param excluactigravpagaimporbienintangservtasa16iva     Exclusivamente de actividades gravadas pagadas en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asoactiapliproppagaimporbienintangservtasa16iva   Asociado a actividades por las cuales se aplicó una proporción pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param ivaacrepagaimporbienintangservtasa16              IVA acreditable pagado en la importación de bienes intangibles y servicios a la tasa del 16%
 * @param excluactigravsumaivaincluimpor                    Exclusivamente de actividades gravadas, suma del IVA incluyendo importaciones
 * @param asoacticuaaplpropsumaivaincluimpor                Asociado a actividades por las cuales se aplicó una proporción, suma del IVA incluyendo importaciones
 * @param ivaacresumaivaincluimpor                          IVA acreditable, suma del IVA incluyendo importaciones
 */
case class IvaAcreditable(excluactgravrfn: JBigDecimal,
                          asoacticualaplproprfn: JBigDecimal,
                          ivaacrerfn: JBigDecimal,
                          excluactgravrfs: JBigDecimal,
                          asoactcualaplproprfs: JBigDecimal,
                          ivaacrerfs: JBigDecimal,
                          excluactgravrf: JBigDecimal,
                          asoactcualaplproprf: JBigDecimal,
                          ivaacrerf: JBigDecimal,
                          excluactgravpagatasa16iva: JBigDecimal,
                          asoacticualapliproppagatasa16iva: JBigDecimal,
                          ivaacrepagatasa16: JBigDecimal,
                          excluactigravsininclimpor: JBigDecimal,
                          asoacticualaplipropsininclimpor: JBigDecimal,
                          ivaacresininclimpor: JBigDecimal,
                          excluactigravpagaimporaduabientangtasa16iva: JBigDecimal,
                          asoacticualapliproppagaimporaduabientangtasa16iva: JBigDecimal,
                          ivaacrepagaimpoaduabientangtasa16iva: JBigDecimal,
                          excluactigravpagaimporbienintangservtasa16iva: JBigDecimal,
                          asoactiapliproppagaimporbienintangservtasa16iva: JBigDecimal,
                          ivaacrepagaimporbienintangservtasa16: JBigDecimal,
                          excluactigravsumaivaincluimpor: JBigDecimal,
                          asoacticuaaplpropsumaivaincluimpor: JBigDecimal,
                          ivaacresumaivaincluimpor: JBigDecimal
                         )


/**
 * IVA No Acreditable.
 *
 * @param asoacticualaplproprfndet                        Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza norte
 * @param asonocumpreqrfn                                 Asociado a que no cumple con requisitos en la región fronteriza norte
 * @param asoactiexerfn                                   Asociado a actividades exentas en la región fronteriza norte
 * @param asoactinoobjrfn                                 Asociado a actividades no objeto en la región fronteriza norte
 * @param totpagarfn                                      Total pagado en la región fronteriza norte
 * @param asoacticualaplproprfsdet                        Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza sur
 * @param asonocumpreqrfs                                 Asociado a que no cumple con requisitos en la región fronteriza sur
 * @param asoactiexerfs                                   Asociado a actividades exentas en la región fronteriza sur
 * @param asoactinoobjrfs                                 Asociado a actividades no objeto en la región fronteriza sur
 * @param totpagarfs                                      Total pagado en la región fronteriza sur
 * @param asoactcualaplproprfdet                          Asociado a actividades por las cuales se aplicó una proporción en la región fronteriza
 * @param asonocumpreqrf                                  Asociado a que no cumple con requisitos en la región fronteriza
 * @param asoactiexerf                                    Asociado a actividades exentas en la región fronteriza
 * @param asoactinoobjrf                                  Asociado a actividades no objeto en la región fronteriza
 * @param totpagarf                                       Total pagado en la región fronteriza
 * @param asoacticualapliproptasa16iva                    Asociado a actividades por las cuales se aplicó una proporción a la tasa del 16% de IVA
 * @param asonocumpreqtasa16iva                           Asociado a que no cumple con requisitos a la tasa del 16% de IVA
 * @param asoactiexetasa16iva                             Asociado a actividades exentas a la tasa del 16% de IVA
 * @param asoactinoobjtasa16iva                           Asociado a actividades no objeto a la tasa del 16% de IVA
 * @param tottasa16iva                                    Total a la tasa del 16% de IVA
 * @param asoactiaplipropivasininclimpor                  Asociado a actividades por las cuales se aplicó una proporción del IVA sin incluir importaciones
 * @param asonocumpreqivasinincimpor                      Asociado a que no cumple con requisitos del IVA sin incluir importaciones
 * @param asoactiexeivasininclimpor                       Asociado a actividades exentas del IVA sin incluir importaciones
 * @param asoactinoobjivasininclimpor                     Asociado a actividades no objeto del IVA sin incluir importaciones
 * @param totivasininclimpor                              Total del IVA sin incluir importaciones
 * @param asoacticualaplipropimporaduabientangtasa16iva   Asociado a actividades por las cuales se aplicó una proporción en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asonocumpreqimporaduabientangtasa16iva          Asociado a que no cumple con requisitos en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoactiexeimporaduabientangtasa16iva            Asociado a actividades exentas en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoactinoobjimporaduabientangtasa16iva          Asociado a actividades no objeto en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param totimporaduabientangtasa16iva                   Total en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoacticualaplipropimporbienintangservtasa16iva Asociado a actividades por las cuales se aplicó una proporción en la importación de bienes intangibles y servicios a la tasa del 16% del IVA
 * @param asonocumpreqimporbienintangservtasa16iva        Asociado a que no cumple con requisitos en la importación de bienes intangibles y servicios a la tasa del 16% del IVA
 * @param asoactiexeimporbienintangservtasa16iva          Asociado a actividades exentas en la importación de bienes intangibles y servicios a la tasa del 16% del IVA
 * @param asoactinoobjimporbienintangservtasa16iva        Asociado a actividades no objeto en la importación de bienes intangibles y servicios a la tasa del 16% del IVA
 * @param totimporbienintangservtasa16iva                 Total en la importación de bienes intangibles y servicios a la tasa del 16% del IVA
 * @param asoacticualaplipropincluimpor                   Asociado a actividades por las cuales se aplicó una proporción incluyendo importaciones
 * @param asonocumpreqincluimpor                          Asociado a que no cumple con requisitos incluyendo importaciones
 * @param asoactiexeincluimpor                            Asociado a actividades exentas incluyendo importaciones
 * @param asoactinoobjincluimpor                          Asociado a actividades no objeto incluyendo importaciones
 * @param totincluimpor                                   Total incluyendo importaciones
 */
case class IvaNoAcreditable(asoacticualaplproprfndet: JBigDecimal,
                            asonocumpreqrfn: JBigDecimal,
                            asoactiexerfn: JBigDecimal,
                            asoactinoobjrfn: JBigDecimal,
                            totpagarfn: JBigDecimal,
                            asoacticualaplproprfsdet: JBigDecimal,
                            asonocumpreqrfs: JBigDecimal,
                            asoactiexerfs: JBigDecimal,
                            asoactinoobjrfs: JBigDecimal,
                            totpagarfs: JBigDecimal,
                            asoactcualaplproprfdet: JBigDecimal,
                            asonocumpreqrf: JBigDecimal,
                            asoactiexerf: JBigDecimal,
                            asoactinoobjrf: JBigDecimal,
                            totpagarf: JBigDecimal,
                            asoacticualapliproptasa16iva: JBigDecimal,
                            asonocumpreqtasa16iva: JBigDecimal,
                            asoactiexetasa16iva: JBigDecimal,
                            asoactinoobjtasa16iva: JBigDecimal,
                            tottasa16iva: JBigDecimal,
                            asoactiaplipropivasininclimpor: JBigDecimal,
                            asonocumpreqivasinincimpor: JBigDecimal,
                            asoactiexeivasininclimpor: JBigDecimal,
                            asoactinoobjivasininclimpor: JBigDecimal,
                            totivasininclimpor: JBigDecimal,
                            asoacticualaplipropimporaduabientangtasa16iva: JBigDecimal,
                            asonocumpreqimporaduabientangtasa16iva: JBigDecimal,
                            asoactiexeimporaduabientangtasa16iva: JBigDecimal,
                            asoactinoobjimporaduabientangtasa16iva: JBigDecimal,
                            totimporaduabientangtasa16iva: JBigDecimal,
                            asoacticualaplipropimporbienintangservtasa16iva: JBigDecimal,
                            asonocumpreqimporbienintangservtasa16iva: JBigDecimal,
                            asoactiexeimporbienintangservtasa16iva: JBigDecimal,
                            asoactinoobjimporbienintangservtasa16iva: JBigDecimal,
                            totimporbienintangservtasa16iva: JBigDecimal,
                            asoacticualaplipropincluimpor: JBigDecimal,
                            asonocumpreqincluimpor: JBigDecimal,
                            asoactiexeincluimpor: JBigDecimal,
                            asoactinoobjincluimpor: JBigDecimal,
                            totincluimpor: JBigDecimal
                           )

/**
 * Datos adicionales.
 *
 * @param ivaretenidocontribupaga                 IVA retenido por el contribuyente pagado
 * @param valactactipagaimporbienservnopagaivaexe Valor de actos o actividades pagados en la importación de bienes y servicios por los que no se pagara el IVA (Exentos)
 * @param valactactipaganopagaivaexedet           Valor de actos o actividades pagados por los que no se pagará el IVA (Exentos)
 * @param valdemaactactipagatasa0iva              Valor de demás actos o actividades pagados a la tasa del 0% de IVA
 * @param valactactinoobjivarealterrnac           Valor de actos o actividades no objeto del IVA realizados en territorio nacional
 * @param valactactinoobjivanocontestabterrnac    Valor de actos o actividades no objeto del IVA por no contar con establecimiento en territorio nacional
 * @param manidioefecfisccompampoperrealprovdet   Manifiesto que se dio efectos fiscales a los comprobantes que amparan las operaciones realizadas con el proveedor, detalle
 */
case class DatosAdicionales(ivaretenidocontribupaga: JBigDecimal,
                            valactactipagaimporbienservnopagaivaexe: JBigDecimal,
                            valactactipaganopagaivaexedet: JBigDecimal,
                            valdemaactactipagatasa0iva: JBigDecimal,
                            valactactinoobjivarealterrnac: JBigDecimal,
                            valactactinoobjivanocontestabterrnac: JBigDecimal,
                            manidioefecfisccompampoperrealprovdet: String
                           )

/**
 * Totales
 *
 * @param totaloperacionesrelaciona        Total de operaciones que relaciona
 * @param determinivaacreasocact           ¿Determinaste IVA acreditable asociado a actividades mixtas?
 * @param montotactacticausimpperiodecla   Monto total de los actos o actividades que causaron el impuesto en el periodo que se declara (actos gravados)
 * @param montotactactireaperdecla         Monto total de los actos o actividades realizados en el periodo que se declara (total de actos incluidos exentos y no objeto)
 * @param montotactacticauimpaniocalinmant Monto total de los actos o actividades que causaron el impuesto en el año de calendario inmediato anterior (actos gravados)
 * @param montotactactireaniocalinmant     Monto total de los actos o actividades realizados en el año de calendario inmediato anterior (total de actos incluidos exentos y no objeto)
 * @param proporcionacreditamiento         Proporción de acreditamiento
 * @param aplicasteestimulosfiscales       ¿Aplicaste estímulos fiscales?
 * @param InfoTotReportados                Información de totales reportados
 * @param ValActActi                       Valor de actos o actividades
 * @param IvaAcreditableTot                Iva Acreditable
 * @param IvaNoAcreditableTot              Iva No Acreditable
 * @param DatosAdicionalestot              Datos adicionales
 */
case class Totales(totaloperacionesrelaciona: JBigDecimal,
                   determinivaacreasocact: String,
                   montotactacticausimpperiodecla: JBigDecimal,
                   montotactactireaperdecla: JBigDecimal,
                   montotactacticauimpaniocalinmant: JBigDecimal,
                   montotactactireaniocalinmant: JBigDecimal,
                   proporcionacreditamiento: String,
                   aplicasteestimulosfiscales: String,
                   InfoTotReportados: Infototreportados,
                   ValActActi: Valactacti,
                   IvaAcreditableTot: Ivaacreditabletot,
                   IvaNoAcreditableTot: Ivanoacreditabletot,
                   DatosAdicionalesTot: DatosadicionalesTot
                  )

/**
 * Información de totales reportados
 *
 * @param valtotactactipagatasa16ivatot                 Valor total de actos o actividades pagados a la tasa del 16% de IVA
 * @param ivapagatasa16tot                              IVA pagado a la tasa del 16%
 * @param montivapaganoacretasa16                       Monto de IVA pagado no acreditable a la tasa del 16%
 * @param valtotactactipagatasa11                       Valor total de actos o actividades pagados a la tasa del 11%
 * @param ivapagatasa11tot                              IVA pagado a la tasa del 11%
 * @param montivapaganoacretasa11                       Monto de IVA pagado no acreditable a la tasa del 11%
 * @param valactactipagarfntot                          Valor de los actos o actividades pagados en la región fronteriza norte
 * @param ivapagarfntot                                 IVA pagado en la región fronteriza norte
 * @param montivapaganoacrerfn                          Monto de IVA pagado no acreditable en la región fronteriza norte
 * @param valactactirfs                                 Valor de los actos o actividades en la región fronteriza sur
 * @param ivapagarfstot                                 IVA pagado en la región fronteriza sur
 * @param montivapaganoacrerfs                          Monto de IVA pagado no acreditable en la región fronteriza sur
 * @param ivapagatrslacontri                            IVA pagado trasladado al contribuyente
 * @param montoivapaganoacretrslacontri                 Monto de IVA pagado no acreditable trasladado al contribuyente
 * @param valactactipagaimpbienservitasa16ivatot        Valor de los actos o actividades pagados en la importación de bienes y servicios a la tasa del 16% de IVA
 * @param ivapagaimpbienservitasa16ivatot               IVA pagado en la importación de bienes y servicios a la tasa del 16%
 * @param montivapaganoacreimporbienservtasa16          Monto de IVA pagado no acreditable en la importación de bienes y servicios a la tasa del 16%
 * @param valactactipagaimpobienservinopagaivaexentotot Valor de los actos o actividades pagados en la importación de bienes y servicios por los que no se pagará el IVA (Exentos)
 * @param valactactipaganopagaivaexentotot              Valor de los actos o actividades pagados por los que no se pagará el IVA (Exentos)
 * @param demasactactipagatasa0iva                      Demás actos o actividades pagados a la tasa del 0% de IVA
 * @param actactinoobjiva                               Actos o actividades no objeto del IVA
 * @param ivapagretecontribu                            IVA pagado retenido por el contribuyente
 * @param ivapagadevdesbonsobadqmercgastgenertot        IVA pagado por devoluciones, descuentos y bonificaciones sobre adquisición de mercancías y gastos en general
 */
case class Infototreportados(valtotactactipagatasa16ivatot: JBigDecimal,
                             ivapagatasa16tot: JBigDecimal,
                             montivapaganoacretasa16: JBigDecimal,
                             valtotactactipagatasa11: JBigDecimal,
                             ivapagatasa11tot: JBigDecimal,
                             montivapaganoacretasa11: JBigDecimal,
                             valactactipagarfntot: JBigDecimal,
                             ivapagarfntot: JBigDecimal,
                             montivapaganoacrerfn: JBigDecimal,
                             valactactirfs: JBigDecimal,
                             ivapagarfstot: JBigDecimal,
                             montivapaganoacrerfs: JBigDecimal,
                             ivapagatrslacontri: JBigDecimal,
                             montoivapaganoacretrslacontri: JBigDecimal,
                             valactactipagaimpbienservitasa16ivatot: JBigDecimal,
                             ivapagaimpbienservitasa16ivatot: JBigDecimal,
                             montivapaganoacreimporbienservtasa16: JBigDecimal,
                             valactactipagaimpobienservinopagaivaexentotot: JBigDecimal,
                             valactactipaganopagaivaexentotot: JBigDecimal,
                             demasactactipagatasa0iva: JBigDecimal,
                             actactinoobjiva: JBigDecimal,
                             ivapagretecontribu: JBigDecimal,
                             ivapagadevdesbonsobadqmercgastgenertot: JBigDecimal
                            )

/**
 * Valor de actos o actividades
 *
 * @param valactactipagarfntota                         Valor de los actos o actividades pagados en la región fronteriza norte
 * @param devdesbonactactipagarfn                       Devoluciones, descuentos y bonificaciones de actos o actividades pagados en la región fronteriza norte
 * @param valnetactactipagarfntot                       Valor neto de actos o actividades pagados en la región fronteriza norte
 * @param ivapagaactactipagarfn                         IVA pagado de actos o actividades pagados en la región fronteriza norte
 * @param valactactipagarfstot                          Valor de actos o actividades pagados en la Región Fronteriza Sur
 * @param devdesbonactactipagarfs                       Devoluciones, descuentos y bonificaciones de actos o actividades pagados en la región fronteriza sur
 * @param valnetactactipagarfstot                       Valor neto de actos o actividades pagados en la región fronteriza sur
 * @param ivapagaactactipagarfs                         IVA pagado de actos o actividades pagados en la región fronteriza sur
 * @param valactactipagarf                              Valor de actos o actividades pagadas en la región fronteriza
 * @param devdesbonactactipagarf                        Devoluciones, descuentos y bonificaciones de actos o actividades pagados en la región fronteriza
 * @param valnetactactipagarftot                        Valor neto de actos o actividades pagados en la región fronteriza
 * @param ivapagaactactipagarf                          IVA pagado de actos o actividades pagados en la región fronteriza
 * @param valactactitotpagatasa16iva                    Valor de actos o actividades de totales pagados a la tasa del 16% de IVA
 * @param devdesbontotpagatasa16iva                     Devoluciones, descuentos y bonificaciones totales pagados a la tasa del 16% de IVA
 * @param valnetactactipagatasa16ivatot                 Valor neto de actos o actividades pagados a la tasa del 16% de IVA
 * @param ivapagatotpagatasa16tot                       IVA pagado de totales pagados a la tasa del 16%
 * @param ivapagasinincluimpor                          IVA pagado sin incluir importaciones
 * @param valactactipagaimporaduabientangtasa16iva      Valor de actos o actividades pagadas en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param devdesbonpagaimporaduabientangtasa16iva       Devoluciones, descuentos y bonificaciones pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param valnetactactipagaimporaduabientangtasa16iva   Valor neto de actos o actividades pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param ivapagaimporaduabientangtasa16tot             IVA pagado en la importación por aduana de bienes tangibles a la tasa del 16%
 * @param valactactipagaimporbienintangservtasa16iva    Valor de actos o actividades pagadas en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param devdesbonpagaimporbienintangservtasa16iva     Devoluciones, descuentos y bonificaciones  pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param valnetactactipagaimporbienintangservtasa16iva Valor neto de actos o actividades pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param ivapagaimporbienintangservtasa16tot           IVA pagado en la importación de bienes intangibles y servicios a la tasa del 16%
 * @param ivapagasumimpvalagreincluimpor                IVA pagado, suma del impuesto al valor agregado incluyendo importaciones
 */
case class Valactacti(valactactipagarfntota: JBigDecimal,
                      devdesbonactactipagarfn: JBigDecimal,
                      valnetactactipagarfntot: JBigDecimal,
                      ivapagaactactipagarfn: JBigDecimal,
                      valactactipagarfstot: JBigDecimal,
                      devdesbonactactipagarfs: JBigDecimal,
                      valnetactactipagarfstot: JBigDecimal,
                      ivapagaactactipagarfs: JBigDecimal,
                      valactactipagarf: JBigDecimal,
                      devdesbonactactipagarf: JBigDecimal,
                      valnetactactipagarftot: JBigDecimal,
                      ivapagaactactipagarf: JBigDecimal,
                      valactactitotpagatasa16iva: JBigDecimal,
                      devdesbontotpagatasa16iva: JBigDecimal,
                      valnetactactipagatasa16ivatot: JBigDecimal,
                      ivapagatotpagatasa16tot: JBigDecimal,
                      ivapagasinincluimpor: JBigDecimal,
                      valactactipagaimporaduabientangtasa16iva: JBigDecimal,
                      devdesbonpagaimporaduabientangtasa16iva: JBigDecimal,
                      valnetactactipagaimporaduabientangtasa16iva: JBigDecimal,
                      ivapagaimporaduabientangtasa16tot: JBigDecimal,
                      valactactipagaimporbienintangservtasa16iva: JBigDecimal,
                      devdesbonpagaimporbienintangservtasa16iva: JBigDecimal,
                      valnetactactipagaimporbienintangservtasa16iva: JBigDecimal,
                      ivapagaimporbienintangservtasa16tot: JBigDecimal,
                      ivapagasumimpvalagreincluimpor: JBigDecimal
                     )

/**
 * Iva Acreditable
 *
 * @param excluactigravpagarfn                                 Exclusivamente de actividades gravadas pagados en la región fronteriza norte
 * @param asoacticualapliproppagarfntot                        Asociado a actividades por las cuales se aplicó una proporción pagados en la región fronteriza norte
 * @param ivaacrepagarfn                                       IVA acreditable pagado en la región fronteriza norte
 * @param excluactigravpagarfs                                 Exclusivamente de actividades gravadas pagados en la región fronteriza sur
 * @param asoacticualapliproppagarfstot                        Asociado a actividades por las cuales se aplicó una proporción pagados en la región fronteriza sur
 * @param ivaacrepagarfs                                       IVA acreditable pagado en la región fronteriza sur
 * @param excluactigravpagarf                                  Exclusivamente de actividades gravadas pagados en la región fronteriza
 * @param asoacticualapliproppagarf                            Asociado a actividades por las cuales se aplicó una proporción pagados en la región fronteriza
 * @param ivaacrepagarf                                        IVA acreditable pagado en la región fronteriza
 * @param excluactigravtotpagatasa16iva                        Exclusivamente de actividades gravadas totales pagados a la tasa del 16% de IVA
 * @param asoacticualapliproppagatasa16ivatot                  Asociado a actividades por las cuales se aplicó una proporción pagados a la tasa del 16% de IVA
 * @param ivaacretotpagatasa16                                 IVA acreditable de totales pagados a la tasa del 16%
 * @param excluactigravivaincluimpor                           Exclusivamente de actividades gravadas del IVA sin incluir importaciones
 * @param asoactiaplipropivasininclimportot                    Asociado a actividades por las cuales se aplicó una proporción del IVA sin incluir importaciones
 * @param ivaacreivasininclimpor                               IVA acreditable del IVA sin incluir importaciones
 * @param excluactigravpagaimporaduabientangtasa16ivatot       Exclusivamente de actividades gravadas pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoacticualapliproppagaimporaduabientangtasa16ivatot Asociado a actividades por las cuales se aplicó una proporción pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param ivaacrepagaimpoaduabientangtasa16ivatot              IVA acreditable pagado en la importación por aduana de bienes tangibles a la tasa del 16%
 * @param excluactigravpagaimporbienintangservtasa16ivatot     Exclusivamente de actividades gravadas pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asoactiapliproppagaimporbienintangservtasa16ivatot   Asociado a actividades por las cuales se aplicó una proporción pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param ivaacrepagaimporbienintangservtasa16tot              IVA acreditable pagados en la importación de bienes intangibles y servicios a la tasa del 16%
 * @param excluactigravivaincluimportot                        Exclusivamente de actividades gravadas del IVA incluyendo importaciones
 * @param asoacticualaplipropivaincluimpor                     Asociado a actividades por las cuales se aplicó una proporción del IVA incluyendo importaciones
 * @param ivaacreincluimpor                                    IVA acreditable incluyendo importaciones
 */
case class Ivaacreditabletot(excluactigravpagarfn: JBigDecimal,
                             asoacticualapliproppagarfntot: JBigDecimal,
                             ivaacrepagarfn: JBigDecimal,
                             excluactigravpagarfs: JBigDecimal,
                             asoacticualapliproppagarfstot: JBigDecimal,
                             ivaacrepagarfs: JBigDecimal,
                             excluactigravpagarf: JBigDecimal,
                             asoacticualapliproppagarf: JBigDecimal,
                             ivaacrepagarf: JBigDecimal,
                             excluactigravtotpagatasa16iva: JBigDecimal,
                             asoacticualapliproppagatasa16ivatot: JBigDecimal,
                             ivaacretotpagatasa16: JBigDecimal,
                             excluactigravivaincluimpor: JBigDecimal,
                             asoactiaplipropivasininclimportot: JBigDecimal,
                             ivaacreivasininclimpor: JBigDecimal,
                             excluactigravpagaimporaduabientangtasa16ivatot: JBigDecimal,
                             asoacticualapliproppagaimporaduabientangtasa16ivatot: JBigDecimal,
                             ivaacrepagaimpoaduabientangtasa16ivatot: JBigDecimal,
                             excluactigravpagaimporbienintangservtasa16ivatot: JBigDecimal,
                             asoactiapliproppagaimporbienintangservtasa16ivatot: JBigDecimal,
                             ivaacrepagaimporbienintangservtasa16tot: JBigDecimal,
                             excluactigravivaincluimportot: JBigDecimal,
                             asoacticualaplipropivaincluimpor: JBigDecimal,
                             ivaacreincluimpor: JBigDecimal
                            )

/**
 * Iva No Acreditable
 *
 * @param asoacticualapliproppagarfntota                       Asociado a actividades por las cuales se aplicó una proporción pagados en la región fronteriza norte
 * @param asonocumpreqpagarfn                                  Asociado a que no cumple con requisitos pagados en la región fronteriza norte
 * @param asoactiexepagarfn                                    Asociado a actividades exentas pagados en la región fronteriza norte
 * @param asoactinoobjpagarfn                                  Asociado a actividades no objeto pagados en la región fronteriza norte
 * @param totactactipagarfn                                    Total de actos o actividades pagados en la región fronteriza norte
 * @param asoactcualapliproppagarfstota                        Asociado a actividades por las cuales se aplicó una proporción pagados en la región fronteriza sur
 * @param asonocumpreqpagarfs                                  Asociado a que no cumple con requisitos pagados en la región fronteriza sur
 * @param asoactiexepagarfs                                    Asociado a actividades exentas pagados en la región fronteriza sur
 * @param asoactinoobjpagarfs                                  Asociado a actividades no objeto pagados en la región fronteriza sur
 * @param totactactipagarfs                                    Total de actos o actividades pagados en la región fronteriza sur
 * @param asoacticualapliproppagarftota                        Asociado a actividades por las cuales se aplicó una proporción  pagados en la región fronteriza
 * @param asonocumpreqpagarf                                   Asociado a que no cumple con requisitos pagados en la región fronteriza
 * @param asoactiexepagarf                                     Asociado a actividades exentas pagados en la región fronteriza
 * @param asoactinoobjpagarf                                   Asociado a actividades no objeto pagados en la región fronteriza
 * @param totactactipagarf                                     Total de actos o actividades pagados en la región fronteriza
 * @param asoacticualapliproptotpagatasa16iva                  Asociado a actividades por las cuales se aplicó una proporción totales pagados a la tasa del 16% de IVA
 * @param asonocumpreqtotpagatasa16iva                         Asociado a que no cumple con requisitos totales pagados a la tasa del 16% de IVA
 * @param asoactiexetotpagatasa16iva                           Asociado a actividades exentas totales pagados a la tasa del 16% de IVA
 * @param asoactinoobjtotpagatasa16iva                         Asociado a actividades no objeto totales pagados a la tasa del 16% de IVA
 * @param totpagatasa16iva                                     Total pagado a la tasa del 16% de IVA
 * @param asoactiaplipropivasininclimportota                   Asociado a actividades por las cuales se aplicó una proporción del IVA sin incluir importaciones
 * @param asonocumpreqivasinincimportot                        Asociado a que no cumple con requisitos del IVA sin incluir importaciones
 * @param asoactiexeivasininclimportot                         Asociado a actividades exentas del IVA sin incluir importaciones
 * @param asoactinoobjivasininclimportot                       Asociado a actividades no objeto del IVA sin incluir importaciones
 * @param totivasininclimportot                                Total del IVA sin incluir importaciones
 * @param asoactcualapliproppagaimporaduabientangtasa16ivatota Asociado a actividades por las cuales se aplicó una proporción pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asonocumpreqpagaimporaduabientangtasa16iva           Asociado a que no cumple con requisitos pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoactiexepagaimporaduabientangtasa16iva             Asociado a actividades exentas pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoactinoobjpagaimporaduabientangtasa16iva           Asociado a actividades no objeto pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param totactactipagaimporaduabientangtasa16ivatot          Total de actos o actividades pagados en la importación por aduana de bienes tangibles a la tasa del 16% de IVA
 * @param asoactiapliproppagaimporbienintangservtasa16ivatota  Asociado a actividades por las cuales se aplicó una proporción pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asonocumpreqpagaimporbienintangservtasa16iva         Asociado a que no cumple con requisitos pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asoactexepagaimporbienintangservtasa16iva            Asociado a actividades exentas pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asoactinoobjpagaimporbienintangservtasa16iva         Asociado a actividades no objeto pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param totactactipagaimporbienintangservtasa16iva           Total de actos o actividades pagados en la importación de bienes intangibles y servicios a la tasa del 16% de IVA
 * @param asoacticualaplipropsumaivaincluimpor                 Asociado a actividades por las cuales se aplicó una proporción en la suma del IVA incluyendo importaciones
 * @param asonocumpreqsumaivaincluimpor                        Asociado a que no cumple con requisitos en la suma del IVA incluyendo importaciones
 * @param asoactiexesumaivaincluimpor                          Asociado a actividades exentas en la suma del IVA incluyendo importaciones
 * @param asoactinoobjsumaivaincluimpor                        Asociado a actividades no objeto en la suma del IVA incluyendo importaciones
 * @param totsumaivainluimporta                                Total de la suma del IVA incluyendo importaciones
 */
case class Ivanoacreditabletot(asoacticualapliproppagarfntota: JBigDecimal,
                               asonocumpreqpagarfn: JBigDecimal,
                               asoactiexepagarfn: JBigDecimal,
                               asoactinoobjpagarfn: JBigDecimal,
                               totactactipagarfn: JBigDecimal,
                               asoactcualapliproppagarfstota: JBigDecimal,
                               asonocumpreqpagarfs: JBigDecimal,
                               asoactiexepagarfs: JBigDecimal,
                               asoactinoobjpagarfs: JBigDecimal,
                               totactactipagarfs: JBigDecimal,
                               asoacticualapliproppagarftota: JBigDecimal,
                               asonocumpreqpagarf: JBigDecimal,
                               asoactiexepagarf: JBigDecimal,
                               asoactinoobjpagarf: JBigDecimal,
                               totactactipagarf: JBigDecimal,
                               asoacticualapliproptotpagatasa16iva: JBigDecimal,
                               asonocumpreqtotpagatasa16iva: JBigDecimal,
                               asoactiexetotpagatasa16iva: JBigDecimal,
                               asoactinoobjtotpagatasa16iva: JBigDecimal,
                               totpagatasa16iva: JBigDecimal,
                               asoactiaplipropivasininclimportota: JBigDecimal,
                               asonocumpreqivasinincimportot: JBigDecimal,
                               asoactiexeivasininclimportot: JBigDecimal,
                               asoactinoobjivasininclimportot: JBigDecimal,
                               totivasininclimportot: JBigDecimal,
                               asoactcualapliproppagaimporaduabientangtasa16ivatota: JBigDecimal,
                               asonocumpreqpagaimporaduabientangtasa16iva: JBigDecimal,
                               asoactiexepagaimporaduabientangtasa16iva: JBigDecimal,
                               asoactinoobjpagaimporaduabientangtasa16iva: JBigDecimal,
                               totactactipagaimporaduabientangtasa16ivatot: JBigDecimal,
                               asoactiapliproppagaimporbienintangservtasa16ivatota: JBigDecimal,
                               asonocumpreqpagaimporbienintangservtasa16iva: JBigDecimal,
                               asoactexepagaimporbienintangservtasa16iva: JBigDecimal,
                               asoactinoobjpagaimporbienintangservtasa16iva: JBigDecimal,
                               totactactipagaimporbienintangservtasa16iva: JBigDecimal,
                               asoacticualaplipropsumaivaincluimpor: JBigDecimal,
                               asonocumpreqsumaivaincluimpor: JBigDecimal,
                               asoactiexesumaivaincluimpor: JBigDecimal,
                               asoactinoobjsumaivaincluimpor: JBigDecimal,
                               totsumaivainluimporta: JBigDecimal
                              )

/**
 * Datos adicionales
 *
 * @param totivaretecontribu                      Total de IVA retenido por el contribuyente
 * @param totactactipagaimporbienservnopagaivaexe Total de actos o actividades pagados en la importación de bienes y servicios por los que no se pagará el IVA (Exentos)
 * @param totactactipaganopagaivaexe              Total de actos o actividades pagados por los que no se pagará el IVA (Exentos)
 * @param demasactactipagatasa0ivatot             Total de demás actos o actividades pagados a la tasa del 0% de IVA
 * @param totactactinoobjivarealiterrinac         Total de actos o actividades no objeto del IVA realizados en territorio nacional
 * @param totactactinoobjivanocontestabterrinac   Total de actos o actividades no objeto del IVA por no contar con establecimiento en territorio nacional
 */
case class DatosadicionalesTot(totivaretecontribu: JBigDecimal,
                               totactactipagaimporbienservnopagaivaexe: JBigDecimal,
                               totactactipaganopagaivaexe: JBigDecimal,
                               demasactactipagatasa0ivatot: JBigDecimal,
                               totactactinoobjivarealiterrinac: JBigDecimal,
                               totactactinoobjivanocontestabterrinac: JBigDecimal
                              )

/**
 * @param fila                          Consecutivo del elemento
 * @param decreto                       Decreto
 * @param valactactiemiaplicestim       Valor de los actos o actividades emitidos a los que les aplica el estímulo fiscal
 * @param ivanocobapliestimfiscal       IVA no cobrado por la aplicación del estímulo fiscal
 * @param valactactireciapliestimfiscal Valor de los actos o actividades recibidos a los que les aplica el estímulo fiscal
 * @param ivanopagapliestifiscal        IVA no pagado por la aplicación del estímulo fiscal
 */
case class DetDatosInformativos(fila: JLong,
                                decreto: String,
                                valactactiemiaplicestim: JBigDecimal,
                                ivanocobapliestimfiscal: JBigDecimal,
                                valactactireciapliestimfiscal: JBigDecimal,
                                ivanopagapliestifiscal: JBigDecimal
                               )

