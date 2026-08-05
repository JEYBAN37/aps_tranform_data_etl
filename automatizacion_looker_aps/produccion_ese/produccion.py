import os
from itertools import groupby

import pandas as pd
from win32ctypes.pywin32.pywintypes import datetime

from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025, URL_CONTRATACION_PLANTILLA
import mysql.connector

from limpieza_datos import extraer_distribucion_redes



def indicadores_salud_publica():
    global cursor

    RANGO_FECHA = datetime.strptime("2026-01-01", "%Y-%m-%d")


    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False  # Disable autocommit
    )

    try:

        url = "bases_downloads/reporte_unificado_2026.csv"
        produccion = pd.read_csv(url)
        # Luego eliminamos las primeras 2 columnas si no las necesitas
        print(produccion.head())

        cursor = connection.cursor()
        #responsables_ebs = cargar_responsables( cursor, DATABASE)
        #connection.commit()

        personas_url = "F:/APS AUTOMATIZACIONES/reportes/2026-06-11/looker/cosolidado_personas_2026-06-11.csv"
        df_personas = pd.read_csv(personas_url)
        df_personas['doc_id'] = df_personas['doc_id'].astype(str).str.strip().str.split(".").str[0]
        df_personas['familia_id'] = df_personas['familia_id'].astype(str).str.strip().str.split(".").str[0]


        df_contratacion = pd.read_csv(URL_CONTRATACION_PLANTILLA)

        df_contratacion['identificacion_contratista'] = df_contratacion['identificacion_contratista'].astype(str).str.strip().str.split(".").str[0]
        df_contratacion['fecha_finalizacion'] = pd.to_datetime(df_contratacion['fecha_finalizacion'], format='%d/%m/%Y',
                                                               errors='coerce')

        df_contratacion_filtrada = df_contratacion[
            df_contratacion['fecha_finalizacion'] >= RANGO_FECHA
            ]


        produccion['Ident Medico'] = produccion['Ident Medico'].astype(str).str.strip().str.split(".").str[0]
        produccion['Edad'] = produccion['Edad'].astype(str).str.strip().str.split(" ").str[0]


        produccion_filtrada = produccion.merge(
            df_contratacion_filtrada[['identificacion_contratista', 'rol_del_contratista']],
            left_on='Ident Medico',
            right_on='identificacion_contratista',
            how='left'
        )
        produccion_filtrada = produccion_filtrada[[
            'Identificacion', 'Tipo ID', 'Nombre Paciente', 'Dir Afil', 'Telefono',
            'Nombre Servicio', 'Cod Diag', 'Cod Diag Rel1', 'Cod Diag Rel2', 'Cod Diag Rel3',
            'Edad', 'Fecha Servicio', 'IPS', 'RED', 'rol_del_contratista','identificacion_contratista','Finalidad','Nombre Medico','Unidad Func','Sexo','programa'
        ]]

        produccion_filtrada = produccion_filtrada[produccion_filtrada['rol_del_contratista'].notna()]

        df_personas_unico = df_personas[['doc_id', 'familia_id']].drop_duplicates(subset=['doc_id'], keep='first')

        # 2. Hacemos el merge con este nuevo dataframe limpio
        produccion_x_familia = produccion_filtrada.merge(
            df_personas_unico,
            left_on='Identificacion',
            right_on='doc_id',
            how='left'
        )

        # 3. (Opcional) Si no quieres que te quede la columna 'doc_id' repetida, la puedes eliminar
        produccion_x_familia = produccion_x_familia.drop(columns=['doc_id'])

        persona = produccion_filtrada[produccion_filtrada['identificacion_contratista'] == '1193458466']


        # diagnosticos_disponibles = produccion_filtrada['Cod Diag'].dropna().unique()
        # servicios_disponibles = produccion_filtrada['Nombre Servicio'].dropna().unique()
        #
        #
        # print(f"Diagnósticos disponibles: {diagnosticos_disponibles}")
        # print(f"Servicios disponibles: {servicios_disponibles}")



        # filtro_diagnostico = produccion_filtrada['Cod Diag'].str.startswith(('Z321', 'Z713', 'Z300', 'Z391', 'Z358', 'Z392'), na=False)
        # filtro_edades = (produccion_filtrada['Edad'] >= 11) & (produccion_filtrada['Edad'] <= 50)
        # filtro_servicios = produccion_filtrada['Nombre Servicio']
        #
        # produccion_filtrada = produccion_filtrada[filtro_diagnostico & filtro_edades & filtro_servicios]
        #
        # agrupado_por_meses_y_conteo = produccion_filtrada.groupby(produccion_filtrada['Fecha'].str[:7])['Identificacion'].nunique().reset_index()
        # agrupado_por_meses_y_conteo.columns = ['Mes', 'Cantidad Personas']
        produccion_x_familia = limpiar_formatos(produccion_x_familia, columnas_fecha=['Fecha Servicio'])

        cargar_csv_a_bigquery(produccion_x_familia, table_id="datos_aps.atenciones_ebs", project_id="aps-project-478903",
                              )

        print(f"Filtrado por contratacion: {len(produccion_x_familia)} filas")


    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()



def unir_reportes_facturacion():
    # una ruta abrir losa rchivo csv o xlsx y unirlos en un solo dataframe
    ruta = r"F:\APS AUTOMATIZACIONES\aps\automatizacion_looker_aps\produccion_ese\bases_downloads\FACTURACION_2026"
    df_principal = pd.DataFrame()

    # mejor concatenación: leer todos los archivos, normalizar columna Identificacion y concatenar
    dfs = []
    for archivo in os.listdir(ruta):
        if archivo.lower().endswith((".csv", ".xlsx", ".xls")):
            path = os.path.join(ruta, archivo)
            try:
                if archivo.lower().endswith(".csv"):
                    df = pd.read_csv(path, skiprows=2, dtype=str,
                                     on_bad_lines='warn'
                                     )
                else:
                    df = pd.read_excel(path, skiprows=2, dtype=str)
            except Exception as e:
                print(f"Skipping {path}: {e}")
                continue
            df = df.iloc[:, 2:]
            dfs.append(df)
            print(f"Processed {archivo} with {len(df)} rows.")

    if not dfs:
        df_principal = pd.DataFrame()
    else:
        df_concat = pd.concat(dfs, ignore_index=True, sort=False)
        # conservar la primera aparición no nula por Identificacion
        df_principal = df_concat
    out_path = os.path.join("bases_downloads", "reporte_unificado_2026.csv")
    df_principal.to_csv(out_path, index=False)

if __name__ == "__main__":
    #unir_reportes_facturacion()
    indicadores_salud_publica()