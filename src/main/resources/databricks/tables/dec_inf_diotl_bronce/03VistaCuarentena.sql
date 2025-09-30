CREATE OR REPLACE VIEW $vistacuarentena$
(
        numeroOperacion COMMENT 'Número de operación', 
        rfc COMMENT 'Registro Federal del Contribuyente',
        fechaPresentacion COMMENT 'Fecha presentación', 
        id_ejecucion COMMENT 'Identificador ejecución',
        blobpath COMMENT 'Ruta del archivo en blob storage', 
        timestampProcesamiento  COMMENT 'Timestamp procesamiento',
        p_fechaPresentacion COMMENT 'Partición de la fecha de presentación truncada al primer día del mes',
        errores COMMENT 'Campo que contiene los errores encontrados en el desdoble del json')
AS
 SELECT  numeroOperacion,
         rfc,
         fechaPresentacion,
         id_ejecucion,
         blobpath,
         timestampProcesamiento,
         p_fechaPresentacion,
         errores
 FROM $bronce$
 WHERE declaracionValida = false;