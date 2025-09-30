// Databricks notebook source
dbutils.widgets.text("nombreArchivo", "", "Nombre Archivo:")

// COMMAND ----------

// MAGIC %run ./Utils

// COMMAND ----------

// MAGIC %run ./DBDatabricks

// COMMAND ----------

// MAGIC %run ./CopyHelper

// COMMAND ----------

import java.sql.DriverManager
val nombreArchivo   = dbutils.widgets.get("nombreArchivo").toString.trim
val nombreTablaListadoUnicoReprocesos = ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaListadoUnicoReprocesos).name
val listadoReingreso = spark.table(ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesLand).name)
.union(spark.table(ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesBronce).name))
.union(spark.table(ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesPlata).name))
.union(spark.table(ConfigurationProvider.obtenerTablaStagingId(EnumStaging.identificadorTablaFaltantesOro).name))
.dropDuplicates("numeroOperacion")
val lista = spark.table(tablaListadoRecepcion).join(listadoReingreso, Seq("numeroOperacion"), "inner")

// COMMAND ----------

// DBTITLE 1,Actualizar estatus de reproceso
val vistaReproceso = "bitacorasreproceso"
val vistaInsumos = "insumosReproceso"
var consulta = 
s"""(SELECT idejecucion, estatus, fechainicio, fechafin
     FROM ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle} 
     WHERE estatus != 0 ) T1"""
val insumos = 
  spark.read.jdbc(url = SqlNpsiCtlContext.jdbcUrl, table = consulta, properties = SqlNpsiCtlContext.connectionProperties)
  .createOrReplaceTempView(vistaReproceso)

spark.sql(s"""SELECT A.numerooperacion, B.estatus FROM ${ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name} A 
              INNER JOIN ${vistaReproceso} B 
               ON A.p_fechapresentacion BETWEEN date_trunc('MM', B.fechainicio) AND date_trunc('MM', B.fechafin)
              WHERE A.idEjecucion = B.idejecucion""")
              .createOrReplaceTempView(vistaInsumos)

spark.sql(s"""MERGE INTO ${nombreTablaListadoUnicoReprocesos} AS T
                USING ${vistaInsumos} AS S
                ON T.numeroOperacion = S.numeroOperacion AND T.estatus != S.estatus
              WHEN MATCHED THEN
                UPDATE SET T.estatus = S.estatus, 
                           T.fechaActualizacion = '${Util.obtenerTimestamp().toString}'""")

// COMMAND ----------

val temporalListadoReproceso = "listadoReproceso"
lista.mapPartitions{partition =>
  partition.map{reg =>{
      println(s"${reg.getString(4)} - ${reg.getString(2)}")
      ListadoUnicoReprocesos(reg.getDate(5).toString,
                             reg.getString(2),
                             reg.getString(0),
                             obtenerTimestamp(),
                             reg.getString(4),
                             0,
                             nombreArchivo,
                             1)
                             }
                }}.toDF.createOrReplaceTempView(temporalListadoReproceso)

// COMMAND ----------

// DBTITLE 1,Se insertan en listadoReproceso o en su defecto se incrementa el campo reprocesos
val temporalListadoReproceso = "listadoReproceso"
lista.mapPartitions{partition =>
  partition.map{reg =>{
      println(s"${reg.getString(4)} - ${reg.getString(2)}")
      ListadoUnicoReprocesos(reg.getDate(5).toString,
                             reg.getString(2),
                             reg.getString(0),
                             obtenerTimestamp(),
                             reg.getString(4),
                             0,
                             nombreArchivo,
                             1)
                             }
                }}.toDF.createOrReplaceTempView(temporalListadoReproceso)

var query = s"""MERGE INTO ${nombreTablaListadoUnicoReprocesos} AS T
                USING ${temporalListadoReproceso} AS S
                ON T.numeroOperacion = S.numeroOperacion
                WHEN MATCHED THEN
                    UPDATE SET T.reprocesos = (T.reprocesos + 1), T.estatus = 0, 
                               T.fechaActualizacion = S.fechaActualizacion, 
                               T.archivoDestino = S.archivoDestino
                WHEN NOT MATCHED THEN
                    INSERT (fechaPresentacion, numeroOperacion, fechaActualizacion, 
                            blobpath, estatus, archivoDestino, reprocesos)
                    VALUES (S.fechaPresentacion, S.numeroOperacion, S.fechaActualizacion,
                            S.blobpath, S.estatus, S.archivoDestino, S.reprocesos);"""
spark.sql(query)
  //registrarListadoUnicoReprocesos(salida.toDF)

// COMMAND ----------

// DBTITLE 1,Se filtran números de operación a filtrar para reprocesar
 spark.table(tablaListadoUnicoReprocesos)
      .filter($"estatus" === 0)      
      .select($"fechaPresentacion", $"numeroOperacion")
      .createOrReplaceTempView("tablaListadoReprocesos")

// COMMAND ----------

// DBTITLE 1,Se obtienen Id Ejecución a reprocesar
var query = s"""SELECT DISTINCT A.id_Ejecucion                   
                FROM ${ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name} A 
                INNER JOIN tablaListadoReprocesos B ON date(B.fechaPresentacion) = date(A.fechadeclaracion)"""
val IdEjecucionAReprocesar = spark.sql(query).map("'" + _.getString(0) + "'").collect.toList.mkString(",")
//val IdEjecucionAReprocesar = "'1E9D17AA-B531-412F-8F6D-73DE607D5898'"

// COMMAND ----------

if(IdEjecucionAReprocesar == null || IdEjecucionAReprocesar.trim == "")
  dbutils.notebook.exit("No hay Id ejecución para configurar un reproceso")

// COMMAND ----------

// DBTITLE 1,Si se descomenta omite la configuración del reproceso
//dbutils.notebook.exit("-----<<<< INFO this a test >>>>-----")

// COMMAND ----------

// DBTITLE 1,Se eliminan Id Ejecución de databricks

// Se elimina de Land
spark.sql(s"""DELETE FROM ${ConfigurationProvider.obtenerTablaLandId(EnumLand.identificadorTablaLand).name} WHERE idEjecucion in (${IdEjecucionAReprocesar})""")

// Se elimina de Bronce
spark.sql(s"""DELETE FROM ${ConfigurationProvider.obtenerTablaBronceId(EnumBronce.identificadorTablaBronce).name} WHERE idEjecucion in (${IdEjecucionAReprocesar})""")

// Se elimina de Ctl
val baseDatosCtl = ConfigurationProvider.identificadorBaseControl
val listaTablasCtl = spark.catalog.listTables(baseDatosCtl).select("name").rdd.map(r => s"${baseDatosCtl}." + r(0).toString).collect().toList
listaTablasCtl.foreach(tabla => 
{  
  if(tabla.contains("cifrascontrol"))
  {
    println(s"Tabla cifras: $tabla")
    spark.sql(s"""DELETE FROM ${tabla} WHERE idEjecucion in (${IdEjecucionAReprocesar})""")
  }
})

// Se elimina de Plata
sat.diot.comunes.EnumsTablas.listaTablasPlata.foreach(tabla =>
{
  println(s"Tabla Plata: ${tabla.name}")
  spark.sql(s"""DELETE FROM ${tabla.name} WHERE idEjecucion in (${IdEjecucionAReprocesar})""")
})

// Se elimina de Oro
sat.diot.comunes.EnumsTablas.listaTablasOro.foreach(tabla =>
{
  println(s"Tabla Oro: ${tabla.name}")
  spark.sql(s"""DELETE FROM ${tabla.name} WHERE idEjecucion in (${IdEjecucionAReprocesar})""")
})

// COMMAND ----------

// DBTITLE 1,Se eliminan Id Ejecución de Citus
val connCitus = DriverManager.getConnection(SqlNpsiCitusContext.jdbcUrlCompleta, SqlNpsiCitusContext.connectionProperties)    
sat.diot.comunes.EnumsTablas.listaTablasOro.foreach(tabla => {
    var queryEliminarCitus = 
    s"""DELETE FROM ${tabla.name.replace(ConfigurationProvider.identificadorBaseOro, ConfigurationProvider.identificadorEsquemaCitus)} 
        WHERE idEjecucion in (${IdEjecucionAReprocesar})"""
    connCitus.prepareStatement(queryEliminarCitus).execute()
    //println(s"Tabla: ${tabla.replace(ConfigurationProvider.identificadorBaseOro, ConfigurationProvider.identificadorEsquemaCitus)}")
})
connCitus.close()

// COMMAND ----------

// DBTITLE 1,Se eliminan Id Ejecución de Single
var queryEliminarSingle = 
s"""DELETE FROM bitacoras.monitoreo_general 
    WHERE idEjecucion IN (${IdEjecucionAReprocesar});
                              
    DELETE FROM ${ConfigurationProvider.identificadorBaseControl}.idejecucion_historico 
    WHERE idEjecucion IN (${IdEjecucionAReprocesar});
    
    UPDATE ${ConfigurationProvider.identificadorBaseControl}.cifras_control 
    SET cifrasrecepcion = -1, cifrasbronce = -1, cifrasbroncevalidos =-1, cifrasbronceerror =-1, 
        cifrasplata =-1, cifrasoro =-1, idestatus = 0, 
        fechaactualizacion = ((now()::timestamp AT TIME ZONE 'America/Mexico_City')::timestamp), 
        esreproceso = true
    WHERE idEjecucion IN (${IdEjecucionAReprocesar});"""

val conn = DriverManager.getConnection(SqlNpsiCtlContext.jdbcUrlCompleta, SqlNpsiCtlContext.connectionProperties)    
conn.prepareStatement(queryEliminarSingle).execute()
conn.close()

// COMMAND ----------

// DBTITLE 1,truncamos tabla temporal
var query = 
s"""TRUNCATE TABLE ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle}_paso"""
val conn = DriverManager.getConnection(SqlNpsiCtlContext.jdbcUrlCompleta, SqlNpsiCtlContext.connectionProperties)    
conn.prepareStatement(query).execute()
conn.close()

// COMMAND ----------

// DBTITLE 1,Insertar en bitácora reprocesos
var consulta = 
s"""(SELECT idEjecucion, fechainicio, fechafin
     FROM ${ConfigurationProvider.identificadorBaseControl}.cifras_control 
     WHERE idEjecucion IN(${IdEjecucionAReprocesar}) ) T1"""
 val insumos = 
  spark.read.jdbc(url = SqlNpsiCtlContext.jdbcUrl, table = consulta, properties = SqlNpsiCtlContext.connectionProperties)
 
  if(!insumos.isEmpty)
  {
    val fechaActual = obtenerTimestamp()

    CopyHelper.copyIn(SqlNpsiCtlContext.jdbcUrlCompleta, 
                      insumos.withColumn("estatus", lit(0))
                             .withColumn("id_proceso", lit(sat.diot.comunes.CatalogoProcesoNPSI.ProcesoDesdobleInformacion))
                             .withColumn("fechainsercion", lit(fechaActual))
                             .withColumn("fechaactualizacion",lit(fechaActual))
                             .withColumnRenamed("idEjecucion", "idejecucion")
                             .coalesce(32),
                      s"${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle}_paso", 
                      true)
    
    query = s"""INSERT INTO ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle} 
                (idejecucion, fechainicio, fechafin, estatus, id_proceso, fechainsercion, fechaactualizacion)
                 SELECT s.idejecucion, s.fechainicio, s.fechafin, s.estatus, s.id_proceso, s.fechainsercion, s.fechaactualizacion 
                 FROM ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle}_paso s
                 LEFT JOIN ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle} t ON s.idejecucion = t.idejecucion AND s.id_proceso = t.id_proceso
                 WHERE t.idejecucion IS NULL;

                UPDATE ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle} t
                SET estatus = 0, 
                    fechaactualizacion = s.fechaactualizacion 
                FROM ${ConfigurationProvider.identificadorTablaBitacoraReprocesosSingle}_paso s
                WHERE t.idejecucion = s.idejecucion AND s.id_proceso = t.id_proceso;"""
                    println(query)
    val conn = DriverManager.getConnection(SqlNpsiCtlContext.jdbcUrlCompleta, SqlNpsiCtlContext.connectionProperties) 
    conn.prepareStatement(query).execute()
    conn.close()    
  }