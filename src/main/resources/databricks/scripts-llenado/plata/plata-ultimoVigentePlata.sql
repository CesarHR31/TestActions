MERGE INTO $ultimavigenteplata$ AS T
USING (SELECT partitionkey, rowkey, timestamp, concepto, rfc, numerooperacion, fechadeclaracion,
            ejercicio, periodicidad, periodo, tipodeclaracion, tipocomplementaria,
            estatusdeclaracion, identificadordeclaracion, identificadordeclaracionpadre,
            identificadordeclaracionraiz, idejecucion, fechacarga, p_timestamp FROM
(SELECT ROW_NUMBER() OVER (PARTITION BY partitionkey, concepto, numerooperacion, fechadeclaracion ORDER BY timestamp DESC) AS row_number,* FROM $ultimavigentebronce$ WHERE $PARTITION_FILTER$)
WHERE row_number = 1) AS S
	 ON T.partitionkey = S.partitionkey AND T.concepto = S.concepto AND T.numerooperacion = S.numerooperacion AND T.fechadeclaracion = S.fechadeclaracion
    WHEN MATCHED THEN
	UPDATE SET T.estatusdeclaracion = S.estatusdeclaracion,
		       T.idejecucion = S.idejecucion,
		       T.fechacarga = S.fechacarga,
		       T.timestamp = S.timestamp,
		       T.p_timestamp = S.p_timestamp
WHEN NOT MATCHED THEN
	INSERT (partitionkey, rowkey, timestamp, concepto, rfc, numerooperacion, fechadeclaracion,
            ejercicio, periodicidad, periodo, tipodeclaracion, tipocomplementaria,
            estatusdeclaracion, identificadordeclaracion, identificadordeclaracionpadre,
            identificadordeclaracionraiz, idejecucion, fechacarga, p_timestamp)
	VALUES (S.partitionkey, S.rowkey, S.timestamp, S.concepto, S.rfc, S.numerooperacion, S.fechadeclaracion,
            S.ejercicio, S.periodicidad, S.periodo, S.tipodeclaracion, S.tipocomplementaria,
            S.estatusdeclaracion, S.identificadordeclaracion, S.identificadordeclaracionpadre,
            S.identificadordeclaracionraiz, S.idejecucion, S.timestamp, S.p_timestamp
	       )