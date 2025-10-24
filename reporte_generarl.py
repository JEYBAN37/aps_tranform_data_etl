# delete
import logging
from datetime import datetime

import jaydebeapi
import pandas as pd

from credenciales import MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DRIVER_PATH, DRIVER_MYSQL, MYSQL_APS, DATABASE
from mysql_conector import ejecutar_consulta_mysql

def query_sociambiental( database = 'agsolutic_aps2024' , where = None):
    qr = where if where else ''
    return f"""
        SELECT * FROM {database}.sociambientals s 
        LEFT JOIN {database}.ubicaciones u 
        ON  u.id = s.ubicacion_id
        {qr}
"""

def query_familias( database = 'agsolutic_aps2024' , where = None):
    qr = where if where else ''
    return f"""
        SELECT f.*   
        FROM {database}.familias f 
        {qr}
"""

def query_poblacion( database = 'agsolutic_aps2024' , where = None):
    qr = where if where else ''
    return f"""
        SELECT
        '{database}' AS origen_db,
        f.id AS id_familia,
        f.sociambiental_id,
        u.microterritorio as microterritorio_,
        s.direccion,
        s.barriovereda,
        s.latitud,
        s.longitud,
        s.fecha,
        j.primerapellido,
        j.segundoapellido,
        j.primernombre,
        j.segundonombre,
        j.fechanac,
        j.tipodocumento,
        j.numerodoc, 
        j.aseguradora,
        j.regimen,
        j.edad,
        j.sexo,
        j.condicioncronica,
        j.canalizacionuno,
        j.canalizaciondos,
        j.canalizaciontres,
        j.observacioncanalizacion,
        s.direccion,    
        s.latitud,
        s.longitud,
        f.celular,
        j.telefono,
        u.*,
        f.tipofamilia,
        f.apgarFuncionalidad,
        o.resultadoEcomapa,
        o.resultadoFamiliograma, 
        o.observacion 
    FROM
        {database}.familias f -- Se especifica la base de datos 'aps2025'
    JOIN
        {database}.juventudadultos j ON f.id = j.familia_id
    JOIN
        {database}.sociambientals s ON f.sociambiental_id = s.id
    JOIN
        {database}.ubicaciones u ON s.ubicacion_id = u.id
    JOIN 
        {database}.observacions o on o.familia_id = f.id 
"""

def reporte_sociambiental(connection_mysql_replica=None, nombre_database = None, nombre_dataframe=None , fecha_corte=None):
    sociambiental_query = ejecutar_consulta_mysql(query_sociambiental(nombre_database),connection_mysql_replica,)
    return pd.DataFrame(sociambiental_query).drop_duplicates()

def reporte_familiar(connection_mysql_replica=None, nombre_database = None, nombre_dataframe=None , fecha_corte=None):
    familiar_query = ejecutar_consulta_mysql(query_familias(nombre_database),connection_mysql_replica,)
    return pd.DataFrame(familiar_query)

def reporte_poblacion(connection_mysql_replica=None, nombre_database = None, nombre_dataframe=None , fecha_corte=None):
    poblacion_query = ejecutar_consulta_mysql(query_poblacion(nombre_database),connection_mysql_replica,)
    return pd.DataFrame(poblacion_query)

def main():
    # crea la conexion a la base de datos replica
    conexion_replica = jaydebeapi.connect(
        DRIVER_MYSQL,
        MYSQL_APS,
        [MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD],
        DRIVER_PATH
    )

    connection_mysql_replica = conexion_replica.cursor()
    df_sociambiental = pd.DataFrame()
    df_familias = pd.DataFrame()
    df_poblacion = pd.DataFrame()
    fecha_reporte = datetime.now().strftime('%Y-%m-%d')
    try:
        # Iterar sobre todas las bases de datos en DATABASE
        for data in DATABASE:
            # Concatenar los datos de cada base de datos
            df_sociambiental = pd.concat(
                [df_sociambiental, reporte_sociambiental(connection_mysql_replica, data, 'reporte_sociambiental', fecha_reporte)],
                ignore_index=True
            )

            df_familias = pd.concat(
                [df_familias, reporte_familiar(connection_mysql_replica, data, 'reporte_familias', fecha_reporte)],
                ignore_index=True
            )

            df_poblacion = pd.concat(
                [df_poblacion, reporte_poblacion(connection_mysql_replica, data, 'reporte_poblacion', fecha_reporte)],
                ignore_index=True
            )

        # Guardar los datos concatenados en archivos CSV
        df_sociambiental.to_csv(f"reportes/{DATABASE[0]}_{fecha_reporte}.csv", index=False)
        df_familias.to_csv(f"reportes/{DATABASE[0]}_familias_{fecha_reporte}.csv", index=False)
        df_poblacion.to_csv(f"reportes/{DATABASE[0]}_poblacion_{fecha_reporte}.csv", index=False)

    except Exception as e:
        logging.error(f"{e}")
        return
    finally:
        connection_mysql_replica.close()
        conexion_replica.close()



if __name__ == "__main__":
    main()
