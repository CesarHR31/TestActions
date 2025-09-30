CREATE TABLE IF NOT EXISTS $cifrascontrolbronce$
(
	`id_ejecucion`               STRING    NOT NULL COMMENT 'Identificador del proceso por el cual entra, ya sea flujo normal o reproceso',
	`nodeclaraciones`            LONG      NOT NULL COMMENT 'Campo que indica los conteos de declaraciones totales',
	`nodeclaracionesdesdobladas` LONG      NOT NULL COMMENT 'Campo que indica los conteos de declaraciones desdobladas',
	`nodeclaracionescuarentena`  LONG      NOT NULL COMMENT 'Campo que indica los conteos de declaraciones en cuarentena',
	`fechainsercion`             TIMESTAMP NOT NULL COMMENT 'fecha de inserción del registro',
	`p_fechainsercion`           TIMESTAMP NOT NULL COMMENT 'Campor partición con la fecha de inserción truncada al primer día del mes'
)
USING DELTA
PARTITIONED BY (p_fechainsercion);