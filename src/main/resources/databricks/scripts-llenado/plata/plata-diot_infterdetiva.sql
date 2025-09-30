INSERT INTO $plata_diot_infterdetiva$
    SELECT DISTINCT
           numeroOperacion,
           rfc,
           fechaPresentacion,
           id_ejecucion,
           payload.IdentiDecla AS identidecla,
           payload.Contribuyente AS contribuyente,
           payload.Declaracion AS declaracion,
           payload.AdminDeclaracion AS admindeclaracion,
           explodedTerDecla AS datosdeltercerodeclarado,
           explodedDatosInfor AS detdatosinformativos,
           FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, "America/Mexico_City") AS fechadesdoble,
           date_trunc('MM', fechaPresentacion) AS p_fechapresentacion
    FROM(
            SELECT *, explode(FILTER(payload.AdminDeclaracion.DatosDelTerceroDeclarado, value -> value IS NOT NULL)) AS explodedTerDecla
            FROM(
                SELECT *, explode(FILTER(payload.AdminDeclaracion.DetDatosInformativos, value -> value IS NOT NULL)) AS explodedDatosInfor
                FROM $vistabronce$
                WHERE (id_ejecucion = '$IDBATCH$' or numeroOperacion in ($NUMEROSOPERACION$)) $PARTITION_FILTER$

            )
    )
    WHERE explodedTerDecla IS NOT NULL

