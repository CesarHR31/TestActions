CREATE TABLE IF NOT EXISTS $controlultimavigente$
(
	 `idejecucion`        STRING       COMMENT 'Identificador de lote de procesamiento',
     `fechainicio`        TIMESTAMP    COMMENT 'Fecha de inicio del delta',
     `fechafinal`         TIMESTAMP    COMMENT 'Fecha final del delta',
     `fechainsercion`     TIMESTAMP    COMMENT 'Fecha de inserción del registro'
)
USING DELTA;