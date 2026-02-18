import ast
import json

import pandas as pd

from automatizacion_looker_aps.query.query_intervenciones import query_intervenciones
from mysql_conector import ejecutar_consulta_mysql

def limpiar_id (df, column_name):
    df[column_name] = (
        df[column_name]
        .fillna('nan')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )
    return df


def filtro_actividades(cursor,df_familia, db, df_personas):
    acumulado_actividades = []

    for database in db:
        intervenciones_query = ejecutar_consulta_mysql(query_intervenciones(database), cursor)
        if not intervenciones_query:
            print(f"No se encontraron personas para {database}. Continuando.")
            continue
        acumulado_actividades.extend(intervenciones_query)

    actividades = acumulado_actividades

    df_actividades_consolidados = pd.DataFrame(actividades)

    df_actividades_consolidados.columns = [desc[0] for desc in cursor.description]

    for i, col in enumerate(df_actividades_consolidados.columns):
        if df_actividades_consolidados.dtypes.iloc[i] == object and col not in (
                'historial','fecha'):
            df_actividades_consolidados.iloc[:, i] = df_actividades_consolidados.iloc[:, i].astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)


    cols_to_clean = ['observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_id']
    for col in cols_to_clean:
        df_actividades_consolidados = limpiar_id(df_actividades_consolidados, col)

    df_actividades_consolidados['fecha'] = pd.to_datetime(df_actividades_consolidados['fecha'],
                                                       errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)


    df_familia['fecha'] = pd.to_datetime(df_familia['fecha'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    df_actividades_consolidados['conteo_nuevas_caracterizaciones'] = df_actividades_consolidados.apply(
        lambda row: verificar_nuevas_caracterizaciones(row, df_familia,df_personas), axis=1
    )



    colums = ['responsable_id', 'fecha','historial','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion', 'conteo_nuevas_caracterizaciones', 'conteo_actualizaciones_ficha']

    df_actividades_consolidados = (
        df_actividades_consolidados
        .groupby(colums)
        .size()
        .reset_index(name='count')
        .sort_values(['responsable_id', 'fecha'], ascending=[True, False]))

    print(f"Caracterizaciones Nuevas {df_actividades_consolidados['conteo_nuevas_caracterizaciones']}")
    print(f"Total de actividades encontradas: {len(df_actividades_consolidados)}")

def verificar_nuevas_caracterizaciones(row, df_familias, df_personas):
    registro_json = row.get('historial', '')
    if isinstance(registro_json, str):
        try:
            registro_json = json.loads(registro_json)
        except Exception:
            try:
                registro_json = ast.literal_eval(registro_json)
            except Exception:
                registro_json = {}
    else:
        registro_json = registro_json or {}

    sociambiental_id = row.get('sociambiental_id')
    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':
        df_familia = df_familias[df_familias['sociambiental_id'] == 125948]

        if not registro_json.get('updateDate') and registro_json.get('fecha') > '2025-12-31':
            if 'validacion' in df_familia.columns:
                no_error_mask = ~df_familia['validacion'].astype(str).str.contains('ERROR EN CARACTERIZACION', na=False)
                count_ok = int(no_error_mask.sum())
                if count_ok > 0:
                    return f" {count_ok} | OK"
                return f" {len(df_familia)} | {df_familia['validacion'].iloc[0]} "


def verificar_actualizacion_ficha(row, df_familias, df_personas):
    registro_json = row.get('historial', '')

    if isinstance(registro_json, str):
        try:
            registro_json = json.loads(registro_json)
        except Exception:
            try:
                registro_json = ast.literal_eval(registro_json)
            except Exception:
                registro_json = {}
    else:
        registro_json = registro_json or {}

    sociambiental_id = row.get('sociambiental_id')
    familia_id = row.get('familia_id')
    observacion_id = row.get('observacion_id')
    fecha_de_modificacion = registro_json.get('fecha')

    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':
        df_familia = df_familias[df_familias['sociambiental_id'] == sociambiental_id]
        if registro_json.get('updateDate') and row.get('fecha') > fecha_de_modificacion:
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE FICHA VIVIENDA "

            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} "

    if ['familia_id'] is not None and familia_id != 'nan':
            df_familia = df_familias[df_familias['familia_id'] == familia_id]
            if row.get('fecha') > df_familia['fecha'].iloc[0]:
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE FICHA FAMILIA"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} "

    if['juventudadultos_id'] is not None and row.get('juventudadultos_id') != 'nan':
        df_persona = df_personas[df_personas['juventudadultos_id'] == row.get('juventudadultos_id')]
        df_familia = df_familias[df_familias['familia_id'] == df_persona['familia_id'].iloc[0]]

        if pd.to_datetime(fecha_de_modificacion, errors='coerce') > (
                pd.to_datetime(df_familia['fecha'].iloc[0], errors='coerce') + pd.Timedelta(days=30)):
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE FICHA PERSONA {df_persona['numerodoc'].iloc[0]}"
            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} "

    if['observacion_id'] is not None and row.get('observacion_id') != 'nan':
        df_familia = df_familias[df_familias['familia_id'] == familia_id]

        if registro_json.get('dirfamilliograma'):
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE OBSERVACION {df_persona['numerodoc'].iloc[0]}"
            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} "












