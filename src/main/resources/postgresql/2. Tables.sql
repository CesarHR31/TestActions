CREATE TABLE IF NOT EXISTS "diot_ctl"."cifras_control" (
	"id_ejecucion" VARCHAR NOT NULL PRIMARY KEY
	,"fechainicio" TIMESTAMP
	,"fechafin" TIMESTAMP
	,"cifrasrecepcion" BIGINT NOT NULL
	,"cifrasbronce" BIGINT NOT NULL
	,"cifrasbroncevalidos" BIGINT NOT NULL
	,"cifrasbronceerror" BIGINT NOT NULL
	,"cifrasplata" BIGINT NOT NULL
	,"cifrasoro" BIGINT NOT NULL
	,"fechainsercion" TIMESTAMP NOT NULL
	,"idestatus" INTEGER NOT NULL
	,"fechaactualizacion" TIMESTAMP NOT NULL
	,"nombrewat" VARCHAR
	,"esreproceso" BOOLEAN
	,"secret_scope" VARCHAR
	,"secret_key" VARCHAR
	,"storage_url" VARCHAR
	);

COMMENT ON COLUMN diot_ctl.cifras_control.id_ejecucion  IS  'Identificador unico de proceso';
COMMENT ON COLUMN diot_ctl.cifras_control.fechainicio  IS  'Fecha inicio del rango de fechas que procesará el batch.';
COMMENT ON COLUMN diot_ctl.cifras_control.fechafin  IS  'Fecha fin del rango de fechas que procesará el batch.';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasrecepcion  IS  'Numero de archivos recibidos por la recepcion';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasbronce  IS  'Numero de archivos procesados en bronce';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasbroncevalidos  IS  'Numero de archivos procesados en bronce validos';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasbronceerror  IS  'Numero de archivos procesados en bronce que presentaron algun error';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasplata  IS  'Numero de archivos procesados en plata en la tabla de caratula';
COMMENT ON COLUMN diot_ctl.cifras_control.cifrasoro  IS  'Numero de archivos procesados en oro en la tabla de caratula';
COMMENT ON COLUMN diot_ctl.cifras_control.fechainsercion  IS  'Fecha de insercion del registro';
COMMENT ON COLUMN diot_ctl.cifras_control.idestatus  IS  'Estatus del batch';
COMMENT ON COLUMN diot_ctl.cifras_control.fechaactualizacion  IS  'Fecha de actualizacion del registro';
COMMENT ON COLUMN diot_ctl.cifras_control.nombrewat IS 'Nombre WAT';
COMMENT ON COLUMN diot_ctl.cifras_control.esreproceso IS 'Fecha de actualizacion del registro';
COMMENT ON COLUMN diot_ctl.cifras_control.secret_scope IS 'Secret scope';
COMMENT ON COLUMN diot_ctl.cifras_control.secret_key IS 'Secret key';
COMMENT ON COLUMN diot_ctl.cifras_control.storage_url IS 'URL HTTPS Ejemplo: https://CUENTA_STORAGE.blob.core.windows.net/CONTENEDOR/';


CREATE TABLE IF NOT EXISTS "diot_ctl"."idejecucion_historico"(
    "id_ejecucion" VARCHAR NOT NULL PRIMARY KEY
    ,"fechaprocesamiento" TIMESTAMP NOT NULL
);

COMMENT ON COLUMN diot_ctl.idejecucion_historico.id_ejecucion  IS  'Identificador del proceso';
COMMENT ON COLUMN diot_ctl.idejecucion_historico.fechaprocesamiento  IS  'Fecha de carga de procesamiento del lote';

--CREATE TABLE IF NOT EXISTS diot_ctl.log(
--   eventdate timestamp  DEFAULT NULL,
--    logger varchar(300),
--    level varchar(100),
--    message varchar(5000),
--    exception varchar(5000)
--);

CREATE TABLE IF NOT EXISTS bitacoras.monitoreo_general
(
    id_ejecucion character varying(36) NOT NULL,
    id_proceso smallint NOT NULL,
    id_paso_ejecucion smallint NOT NULL,
    id_tabla integer NOT NULL,
    registros_origen bigint NOT NULL,
    registros_proceso bigint NOT NULL,
    exitoso boolean NOT NULL,
    tiempo_de_execucion integer NOT NULL,
    detalle_error character varying(5000),
    fecha_ejecucion_proceso timestamp without time zone NOT NULL
);


--CREATE TABLE IF NOT EXISTS diot_ctl.control_gzip
--(
--    id_ejecucion character varying  NOT NULL,
--	nombretabla character varying  NOT NULL,
--	id_tabla_npsi smallint NOT NULL,
--	rutagzip character varying NOT NULL,
--	estatus smallint NOT NULL,
--	fechaactualizacion timestamp without time zone,
--    fecharegistro timestamp without time zone NOT NULL,
--    CONSTRAINT id_ejecucion_control_gzip_pkey PRIMARY KEY (id_ejecucion,nombretabla)
--);

--
--COMMENT ON COLUMN diot_ctl.control_gzip.id_ejecucion
--    IS 'Identificador del proceso';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.nombretabla
--    IS 'Nombre de la tabla del insumo del gzip';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.id_tabla_npsi
--    IS 'Identificador de la tabla dentro del flujo NPSI';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.rutagzip
--    IS 'Ruta de donde se encuentra el gzip';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.estatus
--    IS 'Identificador de estatus';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.fechaactualizacion
--    IS 'Fecha de actualización del registro';
--
--COMMENT ON COLUMN diot_ctl.control_gzip.fecharegistro
--    IS 'Fecha de registro de procesamiento del lote';


CREATE TABLE IF NOT EXISTS diot_ctl.secrets_wat_declaracionconceptofecha
(
    wat character varying  NOT NULL,
	secret character varying  NOT NULL,
	scope character varying  NOT NULL,
	vigente boolean NOT NULL,
	fechaactualizacion timestamp without time zone,
    fecharegistro timestamp without time zone NOT NULL,
    CONSTRAINT wat_control_secret_pkey PRIMARY KEY (wat,secret)
);


COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.wat
    IS 'Nombre de la wat a consultar declaraciones';

COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.secret
    IS 'Nombre del secret en donde esta almacenada la SAS de la WAT a consultar';

COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.scope
    IS 'Identificador del scope en donde se aloja el secret';

COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.vigente
    IS 'Identificador de estatus, indica si esta vigente o no';

COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.fechaactualizacion
    IS 'Fecha de actualización del registro';

COMMENT ON COLUMN diot_ctl.secrets_wat_declaracionconceptofecha.fecharegistro
    IS 'Fecha de registro';

CREATE TABLE IF NOT EXISTS bitacoras.c_pasos_por_ejecucion
(
    id_proceso smallint,
    id_paso smallint,
    orden smallint,
    es_punto_reinicio boolean,
    paso_reinicio smallint,
    notebook_reproceso character varying(100) COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS bitacoras.c_proceso
(
    id_proceso smallint NOT NULL,
    id_tipo_ejecucion smallint NOT NULL,
    descripcion_proceso character varying(100) COLLATE pg_catalog."default" NOT NULL,
    tiempo_estimado smallint,
    id_sistema smallint
);

CREATE TABLE IF NOT EXISTS bitacoras.c_pasos_ejecucion
(
    id_paso_ejecucion smallint NOT NULL,
    descripcion_paso_ejecucion character varying(100) COLLATE pg_catalog."default" NOT NULL
);

CREATE TABLE IF NOT EXISTS bitacoras.c_sistema
(
    id_sistema smallint,
    descripcion_sistema character varying(200) COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS bitacoras.c_tipo_ejecucion
(
    id_tipo_ejecucion smallint NOT NULL,
    descripcion_tipo_ejecucion character varying(100) COLLATE pg_catalog."default" NOT NULL
);

CREATE TABLE IF NOT EXISTS bitacoras_dyp.bitacora_proceso_datos
(
    id_ejecucion character varying(50) COLLATE pg_catalog."default" NOT NULL,
    id_proceso smallint NOT NULL,
    id_paso_ejecucion smallint NOT NULL,
    estatus smallint NOT NULL,
    objeto character varying(100) COLLATE pg_catalog."default",
    registros_procesados bigint NOT NULL,
    fecha_ini_ejecucion timestamp without time zone NOT NULL,
    fecha_fin_ejecucion timestamp without time zone NOT NULL,
    fecha_ini_periodo timestamp without time zone NOT NULL,
    fecha_fin_periodo timestamp without time zone NOT NULL,
    parametros_ejecucion character varying(100) COLLATE pg_catalog."default",
    detalle_error character varying(4000) COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS bitacoras_dyp.bitacora_detalle_paso
(
    id_ejecucion character varying(50) COLLATE pg_catalog."default" NOT NULL,
    id_proceso smallint NOT NULL,
    id_paso_ejecucion smallint NOT NULL,
    detalle_paso character varying(200) COLLATE pg_catalog."default",
    objeto character varying(100) COLLATE pg_catalog."default",
    registros_procesados bigint NOT NULL,
    fecha_ejecucion timestamp without time zone NOT NULL
);


CREATE TABLE IF NOT EXISTS bitacoras_dyp.bitacorareprocesos
(
    idejecucion character varying COLLATE pg_catalog."default" NOT NULL,
    fechainicio timestamp without time zone,
    fechafin timestamp without time zone,
    estatus integer NOT NULL,
    id_proceso integer NOT NULL,
    fechainsercion timestamp without time zone NOT NULL,
    fechaactualizacion timestamp without time zone NOT NULL
);


CREATE TABLE IF NOT EXISTS diot_ctl.config_declaracion_anual_wat
(
 nombrewatconcepto character varying  NOT NULL,
  nombrewatconceptodetalle character varying  NOT NULL,
  secret character varying  NOT NULL,
  scope character varying  NOT NULL,
  vigente boolean NOT NULL,
  fechaactualizacion timestamp without time zone,
  fecharegistro timestamp without time zone NOT NULL,
  CONSTRAINT declaracion_anual_pkey PRIMARY KEY (nombrewatconcepto,secret)
);


COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.nombrewatconcepto
    IS 'Nombre de la wat a consultar conceptos';

 COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.nombrewatconceptodetalle
     IS 'Nombre de la wat a consultar conceptos detalles';

COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.secret
    IS 'Nombre del secret en donde esta almacenada la SAS de la WAT a consultar';

COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.scope
    IS 'Identificador del scope en donde se aloja el secret';

COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.vigente
    IS 'Identificador de estatus, indica si esta vigente o no';

COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.fechaactualizacion
    IS 'Fecha de actualización del registro';

COMMENT ON COLUMN diot_ctl.config_declaracion_anual_wat.fecharegistro
    IS 'Fecha de registro';


INSERT INTO diot_ctl.secrets_wat_declaracionconceptofecha(wat, secret, scope, vigente, fechaactualizacion, fecharegistro)
VALUES
('DeclaracionConceptoFecha', 'diot-connStrTableStorage-OrigenListadoRecepcion', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConceptoFecha', 'diot-connStrTableStorage-OrigenListadoRecepcion2022', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConceptoFecha', 'diot-connStrTableStorage-OrigenListadoRecepcion2023', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConceptoFecha', 'diot-connStrTableStorage-OrigenListadoRecepcion2025', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp));

INSERT INTO diot_ctl.config_declaracion_anual_wat(nombrewatconcepto, nombrewatconceptodetalle, secret, scope, vigente, fechaactualizacion, fecharegistro)
VALUES
('DeclaracionConcepto', 'DeclaracionConceptoDetalle', 'diot-connStrTableStorage-OrigenListadoRecepcion', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConcepto', 'DeclaracionConceptoDetalle', 'diot-connStrTableStorage-OrigenListadoRecepcion2022', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConcepto', 'DeclaracionConceptoDetalle', 'diot-connStrTableStorage-OrigenListadoRecepcion2023', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp)),
('DeclaracionConcepto', 'DeclaracionConceptoDetalle', 'diot-connStrTableStorage-OrigenListadoRecepcion2025', 'NPSI-Declaraciones', true, ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp));
