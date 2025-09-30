CREATE TABLE IF NOT EXISTS $cifrascontroloro$
(
	`id_ejecucion`               STRING    NOT NULL COMMENT 'Identificador del proceso por el cual entra, ya sea flujo normal o reproceso',
	`tabla`                      STRING    NOT NULL COMMENT 'Identificador dela tabla en la que se realizan los conteos',
	`nodeclaraciones`            LONG      NOT NULL COMMENT 'Campo que indica los conteos de declaraciones totales',
	`noregistros`                LONG      NOT NULL COMMENT 'Campo que indica los conteos de registros insertados',
	`fechainsercion`             TIMESTAMP NOT NULL COMMENT 'fecha de inserción del registro',
	`p_fechainsercion`           TIMESTAMP NOT NULL COMMENT 'Campo partición con la fecha de inserción truncada al primer día del mes'
)
USING DELTA
PARTITIONED BY (p_fechainsercion);