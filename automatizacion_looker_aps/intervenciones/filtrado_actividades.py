import ast
import json

import pandas as pd

from automatizacion_looker_aps.query.query_intervenciones import query_intervenciones
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
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


def filtrar_planes_cuidado_creado(df_actividades_consolidados, filtrar_por_tipo='PLAN DE CUIDADO CREADO'):
    df_planes_sin_filtrar = df_actividades_consolidados[
        df_actividades_consolidados['conteo_plan_cuidado'] == filtrar_por_tipo]


    df_planes_sin_filtrar = df_planes_sin_filtrar.sort_values(
        by=['fecha', 'id_familia_plan'],
        ascending=[True, True]
    )

    df_planes_sin_filtrar.drop_duplicates(subset=['fecha', 'id_familia_plan'], keep='first', inplace=True)


    # 2. Ahora que está ordenado, eliminamos los duplicados manteniendo el primero (keep='first')
    df_planes_sin_filtrar = (
        df_planes_sin_filtrar.loc[
            df_planes_sin_filtrar.groupby('id_familia_plan')['fecha'].idxmin()
        ].reset_index(drop=True)
    )

    return df_planes_sin_filtrar

def filtrar_planes_cuidado_verificar_firma(
    df_actividades_consolidados,
    df_planes_firmados,
    filtrar_por_tipo='PLAN DE CUIDADO CREADO',

):
    df_planes = df_actividades_consolidados[
        df_actividades_consolidados['conteo_plan_cuidado'] == filtrar_por_tipo
    ].copy()

    df_planes = df_planes.sort_values(
        by=['id_familia_plan', 'fecha', 'responsable_id'],
        ascending=[True, True, True]
    )

    # If we are filtering CREATED plans and a dataframe of SIGNED plans is provided,
    # keep only created rows that have a matching signed plan with the same responsable_id.

    df_signed_keys = df_planes_firmados[['id_familia_plan', 'responsable_id']].drop_duplicates()
    df_planes = df_planes.merge(df_signed_keys, on=['id_familia_plan', 'responsable_id'], how='inner')

    # drop duplicates keeping the first occurrence
    df_planes = df_planes.drop_duplicates(subset=['fecha', 'id_familia_plan', 'responsable_id'], keep='first').reset_index(drop=True)
    df_planes = df_planes.drop_duplicates(subset=['id_familia_plan', 'responsable_id','conteo_plan_cuidado'], keep='first').reset_index(drop=True)

    return df_planes

def extrer_variable_familia(row, df_familias, variable):

    df_familias['familia_id'] =  df_familias['familia_id'].fillna('nan').astype(str).str.strip().str.replace(r'\.0+$', '', regex=True)
    familia = str(row.get('id_familia_plan')).strip()


    dir_plancuidado = df_familias[
        df_familias['familia_id'] == familia]



    if not dir_plancuidado.empty:
        return dir_plancuidado[variable].iloc[0]
    return '0'


def filtro_actividades(cursor,df_familia, db, df_personas,reporte,client,sheet_id,responsables_ebs):

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

    # df_actividades_consolidados['conteo_actualizaciones_ficha'] = df_actividades_consolidados.apply(
    #     lambda row: verificar_actualizacion_ficha(row, df_familia,df_personas), axis=1
    # )
#
    df_actividades_consolidados['conteo_plan_cuidado'] = df_actividades_consolidados.apply(
        lambda row: verificar_plan_cuidado(row, df_familia), axis=1
    )


    df_actividades_consolidados['id_familia_plan'] = df_actividades_consolidados.apply(
        lambda row: row['conteo_plan_cuidado'].split('|')[-1].strip() if row['conteo_plan_cuidado'] != '0' else '0', axis=1
    )
    familia_A_Comparar = df_actividades_consolidados[df_actividades_consolidados['id_familia_plan'] == '79769']

    df_actividades_consolidados['conteo_plan_cuidado'] = df_actividades_consolidados['conteo_plan_cuidado'].apply(lambda x: x.split('|')[0].strip() if x != '0' else '0')

    familia_A_Comparar = df_actividades_consolidados[df_actividades_consolidados['id_familia_plan'] == '79769']


    colums = ['responsable_id', 'fecha','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion', 'conteo_nuevas_caracterizaciones', 'conteo_plan_cuidado','id_familia_plan']

    # colums = ['responsable_id', 'fecha','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion','conteo_plan_cuidado','id_familia_plan']

    df_actividades_consolidados = (
        df_actividades_consolidados
        .groupby(colums)
        .size()
        .reset_index(name='count')
        .sort_values(['responsable_id', 'fecha'], ascending=[True, True]))


    df_actividades_consolidados = df_actividades_consolidados.sort_values(['fecha', 'responsable_id'], ascending=[True, True])


    # df_actividades_consolidados = df_actividades_consolidados.drop_duplicates(
    #     subset=['responsable_id', 'observacion_id', 'conteo_plan_cuidado'],
    #     keep='first'
    # ).reset_index(drop=True)

    #df_actividades_consolidados = df_actividades_consolidados[df_actividades_consolidados['responsable_id'].isin(['85','74','1465','1580','182','1417'])]
    df_planes_creados = filtrar_planes_cuidado_creado(df_actividades_consolidados, filtrar_por_tipo='PLAN DE CUIDADO CREADO')

    df_planes_firmados = filtrar_planes_cuidado_verificar_firma(df_actividades_consolidados,df_planes_creados, filtrar_por_tipo='PLAN DE CUIDADO FIRMADO')

    df_planes_creados['dir_anexo'] = df_planes_creados.apply(
        lambda row: extrer_variable_familia(row,df_familia,variable='dirfamiliograma'), axis=1
    )

    df_planes_creados['anexo'] = df_planes_creados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='familiograma'), axis=1
    )

    df_planes_firmados['dir_anexo'] = df_planes_firmados.apply(
        lambda row: extrer_variable_familia(row,df_familia,variable='dirplancuidado'), axis=1
    )

    df_planes_firmados['anexo'] = df_planes_firmados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='plancuidado'), axis=1
    )


    #df_planes_firmados.to_csv('reportes.csv', index=False)


    # no poner aqui los que no seanplan de cuidad ni firmado
    df_actualizaciones_ficha = df_actividades_consolidados[~df_actividades_consolidados['conteo_plan_cuidado'].isin(['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'])]

    df_actividades_consolidados = pd.concat([df_planes_firmados, df_planes_creados, df_actualizaciones_ficha], ignore_index=True)

    df_actividades_consolidados['db_anterior'] = df_actividades_consolidados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='db'), axis=1
    )

    cargar_actividades(df_actividades_consolidados, reporte, sheet_id, client)



    #df_responsables_plan_cuidado = df_actividades_consolidados[df_actividades_consolidados['conteo_plan_cuidado'] != '0']

    #df_responsables_plan_cuidado['responsables_ebs'] = df_responsables_plan_cuidado.apply(lambda row: agregar_responsables(row, responsables_ebs, df_familia), axis=1)

    #df_responsables_plan_cuidado['responsables_ebs'] = descomponer_responsables(df_responsables_plan_cuidado)['responsable_asignado']

    #cargar_ebs(df_responsables_plan_cuidado, reporte)


def descomponer_responsables(df_responsables_plan_cuidado):
    expanded_rows = []
    for _, r in df_responsables_plan_cuidado.iterrows():
        if r['responsables_ebs'] != '0':
            responsables_str = r['responsables_ebs']
            names = [s.strip() for s in responsables_str.split('|') if s.strip()]
            for name in names:
                new_row = r.copy()
                new_row['responsable_asignado'] = name
                expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows).reset_index(drop=True)


def cargar_ebs (df_responsables_plan_cuidado, reporte):
    df_responsables_plan_cuidado.to_csv(F'../reportes/{reporte}/looker/responsables_plan_cuidado_{reporte}.csv',
                                       index=False)

    cargar_csv_a_bigquery(df_responsables_plan_cuidado, table_id="datos_aps.ebs_planes_cuidado",
                          project_id="aps-project-478903",
                          columnas_fecha=['fecha'])


def cargar_actividades(df_actividades_consolidados, reporte, sheet_id, client):
    df_actividades_consolidados =limpiar_formatos(df_actividades_consolidados, columnas_fecha=['fecha'])
    cargar_csv_a_bigquery(df_actividades_consolidados, table_id="datos_aps.actividades",
                          project_id="aps-project-478903")

    df_actividades_consolidados.to_csv(F'../reportes/{reporte}/looker/consolidado_actividades_{reporte}.csv',
                                       index=False)
    df_actividades_consolidados = df_actividades_consolidados.drop(['observacion_id', 'familia_id','sociambiental_id','juventudadultos_id'], axis=1)
    sobrescribir_hoja(sheet_id, "ACTIVIDADES", df_actividades_consolidados, client)

def verificar_nuevas_caracterizaciones(row, df_familias):
    registro_json = json_to_dict(row)

    sociambiental_id = row.get('sociambiental_id')
    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':
        df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

        if not registro_json.get('updateDate') and  row.get('fecha') > '2025-12-31' and not df_familia.empty:
            if 'validacion' in df_familia.columns:
                no_error_mask = ~df_familia['validacion'].astype(str).str.contains('ERROR EN CARACTERIZACION', na=False)
                count_ok = int(no_error_mask.sum())
                if count_ok > 0:
                    return f" {count_ok} | OK"
                return f" {len(df_familia)} | {df_familia['validacion'].iloc[0]} "
    return '0'


def verificar_actualizacion_ficha(row, df_familias, df_personas):

    try:
        registro_json = json_to_dict(row)

        sociambiental_id = row.get('sociambiental_id') if row.get('sociambiental_id') is not None else 'nan'
        familia_id = row.get('familia_id') if row.get('familia_id') is not None else 'nan'
        juventudadultos_id = row.get('juventudadultos_id') if row.get('juventudadultos_id') is not None else 'nan'
        fecha_de_modificacion = registro_json.get('fecha') if registro_json.get('fecha') else '1900-01-01'

        fecha_de_fila = row.get('fecha')  if row.get('fecha') else '1900-01-01'
        fehca_Actualizacion_fila = registro_json.get('updateDate') if registro_json.get('updateDate') else False

        observacion_id = row.get('observacion_id') if row.get('observacion_id') is not None else 'nan'
        dir_familiograma = registro_json.get('dirfamilliograma') if registro_json.get('dirfamilliograma') else None

        if ['sociambiental_id'] is not None and sociambiental_id != 'nan':


            df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE |C"

            if fehca_Actualizacion_fila and fecha_de_fila > fecha_de_modificacion:
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

            if pd.to_datetime(fecha_de_fila, errors='coerce') > (
                    pd.to_datetime(fecha_creacion, errors='coerce') + pd.Timedelta(days=30)):
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE FICHA PERSONA {df_persona['doc_id'].iloc[0]} | O"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} | O"

        if['observacion_id'] is not None and observacion_id != 'nan' and familia_id != 'nan' :

            df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE  |O"

            if dir_familiograma is not None:
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE OBSERVACION {df_familia['id'].iloc[0]} |O"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |O"

        return '0'
    except Exception as e:
        print(f"Error en verificar_actualizacion_ficha: {e}")
        return '0'


def json_to_dict(row,column_name='historial'):
    registro_json = row.get(column_name , '')

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
    try:
        registro_json = json_to_dict(row)

        observacion_id = row.get('observacion_id')

        plan_de_cuidado = registro_json.get('plancuidado')

        if ['observacion_id'] is not None and observacion_id != 'nan' and not registro_json.get('dirfamilliograma'):
            familia_id = registro_json.get('familia_id')
            df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

            if df_familia.empty:
                return f"PLAN DE CUIDADO NO VALIDO  | {familia_id}"

            if df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f"PLAN DE CUIDADO CON {str(df_familia['validacion'].iloc[0]).strip()}   | {familia_id}"
            if plan_de_cuidado :
                return f"PLAN DE CUIDADO FIRMADO | {familia_id}"
            if registro_json.get('actividaddesarrollar'):
                return f"PLAN DE CUIDADO CREADO | {familia_id}"
            if registro_json.get('actividaddesarrollar') == '':
                    return f"PLAN DE CUIDADO NO VALIDO | {familia_id}"
            else :
                return f"0"

        return '0'
    except Exception as e:
        print(f"Error en verificar_plan_cuidado: {e}")
        return f"PLAN DE CUIDADO NO VALIDO ERROR"


def agregar_responsables(row, df_responsables,df_familias):

    if row.get('conteo_plan_cuidado') == 'PLAN DE CUIDADO FIRMADO':
        id_familia_plan = row.get('id_familia_plan')
        df_familia = df_familias[df_familias['familia_id'] == int(float(id_familia_plan))]

        if not df_familia.empty:
            ebs = df_familia['involucrado_plan_cuidado']

            if pd.isna(ebs) or ebs == 'nan' or ebs.strip() == '':
                return "0"

            array_validacion = ebs.split('|') if pd.notna(ebs) else []

            responsables = ''

            for i in range(len(array_validacion)):
                array_validacion[i] = array_validacion[i].strip()
                responsable = df_responsables[df_responsables['id'].isin(i)]
                if not responsable.empty:
                    responsables += responsable['nombre'].iloc[0] + ' | '

            return responsables
    return "0"









