CREATE TABLE IF NOT EXISTS $land$ (
    fechacarga STRING NOT NULL                            COMMENT 'Particion Key de la declaracion en la WAT en formato YYYYMMDDHHMMSS',
    id_ejecucion STRING NOT NULL                          COMMENT 'Identificador de lote de procesamiento',
    rfc STRING NOT NULL                                   COMMENT 'Registro federal del contribuyente en la declaracion',
    numerooperacion LONG NOT NULL                         COMMENT 'Identificador unico de la declaracion',
    obligaciones STRING                                   COMMENT 'Obligaciones presentadas en la declaracion',
    fechadeclaracion TIMESTAMP NOT NULL                   COMMENT 'Fecha de presentacion de la declaracion',
    ejercicio INT NOT NULL                                COMMENT 'Ejercicio durante el que se presento la declaracion',
    blobpath STRING NOT NULL                              COMMENT 'Ruta donde se almacena el archivo JSON de la declaracion en la cuenta de almacenamiento',
    p_fechapresentacion DATE NOT NULL                     COMMENT 'Particion en formato fecha YYYY-MM-DD'

)
USING DELTA
PARTITIONED BY (p_fechapresentacion);