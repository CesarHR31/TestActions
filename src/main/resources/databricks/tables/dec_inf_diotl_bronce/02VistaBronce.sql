CREATE OR REPLACE VIEW $vistabronce$
(
        numeroOperacion COMMENT 'Número de operación', 
        rfc COMMENT 'Registro Federal del Contribuyente',
        fechaPresentacion COMMENT 'Fecha presentación', 
        id_ejecucion COMMENT 'Identificador ejecución',
        blobpath COMMENT 'Ruta del archivo en blob storage', 
        timestampProcesamiento  COMMENT 'Timestamp procesamiento',
        p_fechaPresentacion COMMENT 'Partición de la fecha de presentación truncada al primer día del mes',
        errores COMMENT 'Errores',
        payload COMMENT 'Estructura del json de la declaración'
)
AS
 SELECT  numeroOperacion,
         rfc,
         fechaPresentacion,
         id_ejecucion,
         blobpath,
         timestampProcesamiento,
         p_fechaPresentacion,
         errores,
         payload
 FROM $bronce$
 WHERE declaracionValida = true;