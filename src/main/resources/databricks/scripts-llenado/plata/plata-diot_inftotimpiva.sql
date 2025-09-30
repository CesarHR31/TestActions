INSERT INTO $plata_diot_inftotimpiva$
    SELECT DISTINCT numeroOperacion,
           rfc,
           fechaPresentacion,
           id_ejecucion,
           payload.IdentiDecla AS identidecla,
           payload.Contribuyente AS contribuyente,
           payload.Declaracion AS declaracion,
           payload.AdminDeclaracion AS admindeclaracion,
           explodedAdmin.Totales AS totales,
           FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, "America/Mexico_City") AS fechadesdoble,
           date_trunc('MM', fechaPresentacion) AS p_fechapresentacion
    FROM(
            SELECT *, explode(payload.AdminDeclaracion) AS explodedAdmin
            FROM $vistabronce$
            WHERE size(payload.AdminDeclaracion.Totales) > 0
            AND (id_ejecucion = '$IDBATCH$' or numeroOperacion in ($NUMEROSOPERACION$)) $PARTITION_FILTER$
    )
    WHERE explodedAdmin.Totales IS NOT NULL