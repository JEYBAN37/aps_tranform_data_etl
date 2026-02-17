import pandas as pd

from automatizacion_looker_aps.query.query_familia import query_familia
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from export_aps_124 import limpiar_formato_longitud, limpiar_formato_latitud
from mysql_conector import ejecutar_consulta_mysql


def cargar_familias(cursor, df_distribucion_redes,FE_REPORTE,client,db,sheet_id):
    acumulado_familias = []

    for database in db:
        familias_query = ejecutar_consulta_mysql(query_familia(database), cursor)
        if not familias_query:
            print(f"No se encontraron personas para {database}. Continuando.")
            continue
        acumulado_familias.extend(familias_query)

    # assign accumulated results back to `familias_query` so later code can build the DataFrame
    familias = acumulado_familias

    df_familias_consolidados = pd.DataFrame(familias)
    df_observaciones_consolidados = pd.DataFrame(familias)

    df_familias_consolidados.columns = [desc[0] for desc in cursor.description]
    df_observaciones_consolidados.columns = [desc[0] for desc in cursor.description]

    # dividir reporte Observacion duplicados y no duplicados
    df_familias_consolidados.drop_duplicates(subset=['familia_id', 'db'], keep='first', inplace=True)

    print("Limpieza de datos completada.")
    df_familias_consolidados['longitud'] = df_familias_consolidados['longitud'].apply(limpiar_formato_longitud)
    df_familias_consolidados['latitud'] = df_familias_consolidados['latitud'].apply(limpiar_formato_latitud)

    for i, col in enumerate(df_familias_consolidados.columns):
        if df_familias_consolidados.dtypes.iloc[i] == object and col not in (
                'longitud', 'latitud', 'familiograma', 'plancuidado'):
            df_familias_consolidados.iloc[:, i] = df_familias_consolidados.iloc[:, i].astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

    for i, col in enumerate(df_observaciones_consolidados.columns):
        if df_observaciones_consolidados.dtypes.iloc[i] == object and col not in ('longitud', 'latitud'):
            df_observaciones_consolidados.iloc[:, i] = df_observaciones_consolidados.iloc[:, i].astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

    df_familias_consolidados['fecha'] = pd.to_datetime(df_familias_consolidados['fecha'],
                                                       errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    df_familias_consolidados['responsable_numero'] = (
        df_familias_consolidados['responsable_numero']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )

    df_familias_consolidados['resposable_plancuidado_id'] = (
        df_familias_consolidados['resposable_plancuidado_id']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )

    df_familias_consolidados['validacion'] = ''
    mask_missing = df_familias_consolidados['longitud'].isna() | df_familias_consolidados['latitud'].isna()
    df_familias_consolidados.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (COORDENADAS INVALIDAS)'

    mask_missing = df_familias_consolidados['total_personas_cursos_vida'] == 0
    df_familias_consolidados.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (SIN INTEGRANTES)'

    df_familias_consolidados['redes'] = df_familias_consolidados['territorio'].map(
        df_distribucion_redes.set_index('TERRITORIO')['RED']
    ).fillna('')

    df_familias_consolidados.to_csv(
        F'../reportes/{FE_REPORTE}/looker/cosolidado_familias_{FE_REPORTE}.csv')

    df_observaciones_consolidados.to_csv(
        F'../reportes/{FE_REPORTE}/looker/cosolidado_observaciones_{FE_REPORTE}.csv')

    df_familias_consolidados['reporte_fecha'] = FE_REPORTE

    cargar_csv_a_bigquery(df_familias_consolidados, table_id="datos_aps.familias", project_id="aps-project-478903",
                          columnas_fecha=['date', 'fecha', 'reporte_fecha'])

    sobrescribir_hoja(sheet_id, "cosolidado_familias", df_familias_consolidados,
                    client)