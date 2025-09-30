CREATE TABLE IF NOT EXISTS $partitionkeyrfcobligacionesultimavigente$
(
    `particionrfcobligacion`        STRING    NOT NULL COMMENT 'Partición del rfc por obligación',
    `fechaactualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se actualiza el registro',
    `fechainsercion`      TIMESTAMP NOT NULL COMMENT 'Fecha en la que se inserta el registro'
)
USING DELTA;