INSERT INTO $ultimavigenteoro$
 SELECT partitionkey,
            rowkey,
            timestamp,
            concepto,
            rfc,
            numerooperacion,
            fechadeclaracion,
            ejercicio,
            periodicidad,
            periodo,
            tipodeclaracion,
            tipocomplementaria,
            estatusdeclaracion,
            identificadordeclaracion,
            identificadordeclaracionpadre,
            identificadordeclaracionraiz,
            idejecucion,
            fechacarga,
            p_timestamp
 FROM $ultimavigenteplata$
 WHERE estatusdeclaracion = 1;