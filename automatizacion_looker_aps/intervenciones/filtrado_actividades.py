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
    df_familia = pd.read_csv('df_familia.csv')  # Cargar df_familia para depuración
    df_personas = pd.read_csv('df_personas.csv')  # Cargar df_personas para depuración
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
        lambda row: verificar_nuevas_caracterizaciones(row, df_familia), axis=1
    )

    df_actividades_consolidados['conteo_actualizaciones_ficha'] = df_actividades_consolidados.apply(
        lambda row: verificar_actualizacion_ficha(row, df_familia,df_personas), axis=1
    )

    df_actividades_consolidados['conteo_plan_cuidado'] = df_actividades_consolidados.apply(
        lambda row: verificar_plan_cuidado(row, df_familia), axis=1
    )

    colums = ['responsable_id', 'fecha','historial','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion', 'conteo_nuevas_caracterizaciones', 'conteo_actualizaciones_ficha','conteo_plan_cuidado']

    df_actividades_consolidados = (
        df_actividades_consolidados
        .groupby(colums)
        .size()
        .reset_index(name='count')
        .sort_values(['responsable_id', 'fecha'], ascending=[True, False]))

    # organizar las fechas por mas antiguo a mas reciente para cada responsable
    df_actividades_consolidados = df_actividades_consolidados.sort_values(['responsable_id', 'fecha'], ascending=[True, True])

    #df_actividades_consolidados = df_actividades_consolidados.drop_duplicates(subset=['responsable_id', 'fecha'], keep='first')

    print(f"Caracterizaciones Nuevas {df_actividades_consolidados['conteo_nuevas_caracterizaciones']}")
    print(f"Total de actividades encontradas: {len(df_actividades_consolidados)}")

def verificar_nuevas_caracterizaciones(row, df_familias):
    registro_json = json_to_dict(row)

    sociambiental_id = row.get('sociambiental_id')
    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':
        print(f"Verificando nueva caracterización para sociambiental_id: {sociambiental_id}")
        df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

        if not registro_json.get('updateDate') and registro_json.get('fecha') > '2025-12-31' and not df_familia.empty:
            if 'validacion' in df_familia.columns:
                no_error_mask = ~df_familia['validacion'].astype(str).str.contains('ERROR EN CARACTERIZACION', na=False)
                count_ok = int(no_error_mask.sum())
                if count_ok > 0:
                    return f" {count_ok} | OK"
                return f" {len(df_familia)} | {df_familia['validacion'].iloc[0]} "
    return '0'


def verificar_actualizacion_ficha(row, df_familias, df_personas):
    registro_json = json_to_dict(row)

    sociambiental_id = row.get('sociambiental_id')
    familia_id = row.get('familia_id')
    juventudadultos_id = row.get('juventudadultos_id')
    fecha_de_modificacion = registro_json.get('fecha')

    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':


        df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

        if df_familia.empty:
            return " 1 | VERIFICAR FAMILIA NO TIENE |C"

        if registro_json.get('updateDate') and row.get('fecha') > fecha_de_modificacion:
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE FICHA VIVIENDA |C"

            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |C"

    if ['familia_id'] is not None and familia_id != 'nan':
            df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE |C"

            if row.get('fecha') > df_familia['fecha'].iloc[0]:
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE FICHA FAMILIA |C"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |C"


    if['juventudadultos_id'] is not None and juventudadultos_id != 'nan':
        df_personas['id'] = df_personas['id'].fillna('nan').astype(str).str.strip().str.replace(r'\.0+$', '', regex=True)
        df_persona = df_personas[df_personas['juventud_id'] == int(float(juventudadultos_id))]

        if df_persona.empty:
            return " 1 | VERIFICAR PERSONA NO EXISTE |C"

        df_familia = df_familias[df_familias['familia_id'] == df_persona['familia_id'].iloc[0]]

        if df_familia.empty:
            return " 1 | VERIFICAR FAMILIA NO TIENE  |C"

        fecha_creacion = df_familia['fecha'].iloc[0]
        fe = fecha_de_modificacion

        if pd.to_datetime(row.get('fecha'), errors='coerce') > (
                pd.to_datetime(fecha_creacion, errors='coerce') + pd.Timedelta(days=30)):
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE FICHA PERSONA {df_persona['doc_id'].iloc[0]} | O"
            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} | O"

    if['observacion_id'] is not None and row.get('observacion_id') != 'nan':

        df_familia = df_familias[df_familias['familia_id'] == int(float(registro_json.get('familia_id')))]

        if df_familia.empty:
            return " 1 | VERIFICAR FAMILIA NO TIENE  |O"

        if registro_json.get('dirfamilliograma'):
            if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f" 1 | ACTUALIZACION DE OBSERVACION {df_familia['id'].iloc[0]} |O"
            return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |O"

    return '0'


def json_to_dict(row):
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

    return registro_json

def verificar_plan_cuidado(row, df_familias):
    registro_json = json_to_dict(row)

    observacion_id = row.get('observacion_id')

    plan_de_cuidado = registro_json.get('plancuidado')

    if ['observacion_id'] is not None and observacion_id != 'nan' and not registro_json.get('dirfamilliograma'):
        df_familia = df_familias[df_familias['familia_id'] == int(float(registro_json.get('familia_id')))]

        if df_familia.empty:
            return " 1 | PLAN DE CUIDADO NO VALIDO  "

        if df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
            return f" 1 | PLAN DE CUIDADO CON {str(df_familia['validacion'].iloc[0]).strip()} "
        if plan_de_cuidado :
            return f" 1 | PLAN DE CUIDADO FIRMADO"
        else:
            return f" 1 | PLAN DE CUIDADO CREADO"

    return '0'








