CREATE TABLE IF NOT EXISTS $conciliacion$
(
	`fechainicial`                     TIMESTAMP  NOT NULL COMMENT 'Fecha inicial en donde inicia el rango de la conciliación a efectuarse con formato YYYY-MM-DDTHH:mm:ss',
	`fechafinal`                       TIMESTAMP  NOT NULL COMMENT 'Fecha final en donde inicia el rango de la conciliación a efectuarse con formato YYYY-MM-DDTHH:mm:ss',
	`modeloaconciliar`                 STRING     NOT NULL COMMENT 'Nombre del modelo que se esta conciliando(oro,plata)',
	`tablaaconciliar`                  STRING     NOT NULL COMMENT 'Nombre de la tabla que se conciliara',
	`duplicados`                       LONG       NOT NULL COMMENT 'Conteo de duplicados en la tabla a conciliar',
	`conteoinicialesperado`            LONG       NOT NULL COMMENT 'Conteo inicial de la tabla que sirve de base para los totales',
	`conteoinicialactual`              LONG       NOT NULL COMMENT 'Conteo inicial de la tabla a conciliar',
	`diferenciainicial`                LONG       NOT NULL COMMENT 'Diferencia entre conteoinicialesperado-conteoinicialactual',
	`conteofinalesperado`              LONG                COMMENT 'Conteo final de la tabla que sirve de base para los totales',
	`conteofinalactual`                LONG                COMMENT 'Conteo final de la tabla a conciliar',
	`diferenciafinal`                  LONG                COMMENT 'Diferencia entre conteofinalesperado-conteofinalactual',
	`procesorealizado`                 STRING     NOT NULL COMMENT 'Indica la acción ejecutada en la conciliación CONTEOS_IGUALES, CONCILIACION_EJECUTADA',
	`exitoso`                          BOOLEAN    NOT NULL COMMENT 'Campo que indica si el proceso fue exitoso',
   	`error`                            STRING              COMMENT 'Campo que indica el error en caso de existir',
	`tiempoenminutostardoenprocesar`   LONG       NOT NULL COMMENT 'Tiempo en minutos que tarda en realizar todo el proceso desde el inicio hasta el final',
	`timestampinsercionregistro`       TIMESTAMP  NOT NULL COMMENT 'Indica la fecha en el que se guarda el registro en formato YYYY-MM-DDTHH:mm:ss'
)
USING DELTA;