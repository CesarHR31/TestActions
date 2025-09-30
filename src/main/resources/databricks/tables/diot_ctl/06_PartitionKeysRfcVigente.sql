CREATE TABLE IF NOT EXISTS $partitionkeyrfcultimavigente$
(
    `particionrfc`        STRING    NOT NULL COMMENT 'Partición del rfc',
    `fechaactualizacion`  TIMESTAMP NOT NULL COMMENT 'Fecha en la que se actualiza el registro',
    `fechainsercion`      TIMESTAMP NOT NULL COMMENT 'Fecha en la que se inserta el registro'
    )
USING DELTA;