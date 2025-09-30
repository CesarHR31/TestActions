CREATE TABLE IF NOT EXISTS $ultimavigenteplata$ (
    partitionkey STRING                   COMMENT 'Particion Key de la declaracion anual en la WAT',
    rowkey STRING                         COMMENT 'Particion RowKey de la declaracion anual en la WAT',
    timestamp Timestamp                   COMMENT 'Identificador de marca de tiempo en la WAT',
    concepto STRING                       COMMENT 'Identificador de concepto de la declaración',
    rfc STRING                            COMMENT 'Registro federal del contribuyente en la declaracion',
    numerooperacion LONG                  COMMENT 'Identificador unico de la declaracion',
    fechadeclaracion TIMESTAMP            COMMENT 'Fecha de la Declaracion',
    ejercicio INT                         COMMENT 'Ejercicio de la Declaracion',
    periodicidad STRING                   COMMENT 'Periodicidad',
    periodo STRING                        COMMENT 'Periodo',
    tipodeclaracion STRING                COMMENT 'TipoDeclaracion',
    tipocomplementaria STRING             COMMENT 'TipoComplementaria',
    estatusdeclaracion INT                COMMENT 'EstatusDeclaracion',
    identificadordeclaracion STRING       COMMENT 'IdentificadorDeclaracion',
    identificadordeclaracionpadre STRING  COMMENT 'IdentificadorDeclaracionPadre',
    identificadordeclaracionraiz STRING   COMMENT 'IdentificadorDeclaracionRaiz',
    idejecucion STRING                    COMMENT 'Identificador de lote de procesamiento',
    fechacarga TIMESTAMP                  COMMENT 'Fecha en la que se inserta registro',
    p_timestamp DATE                      COMMENT 'Particion en formato fecha YYYY-MM-DD'
)
USING DELTA
PARTITIONED BY (p_timestamp);