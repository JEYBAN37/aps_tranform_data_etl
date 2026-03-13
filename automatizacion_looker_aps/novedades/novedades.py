import pandas as pd

from automatizacion_looker_aps.query.query_novedades import query_novedades
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from mysql_conector import ejecutar_consulta_mysql


def cargar_novedades(cursor, df_distribucion_redes,FE_REPORTE,client,db,sheet_id):
    acumulado_novedades = []
    for database in db:
        novedades_query = ejecutar_consulta_mysql(query_novedades(database), cursor)
        if not novedades_query:
            print(f"No se encontraron personas para {database}. Continuando.")
            continue
        acumulado_novedades.extend(novedades_query)

    df_novedades_consolidado = pd.DataFrame(acumulado_novedades)
    df_novedades_consolidado.columns = [desc[0] for desc in cursor.description]

    for i, col in enumerate(df_novedades_consolidado.columns):
        if df_novedades_consolidado.dtypes.iloc[i] == object:
            df_novedades_consolidado.iloc[:, i] = df_novedades_consolidado.iloc[:, i].astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

    df_novedades_consolidado['fecha'] = pd.to_datetime(df_novedades_consolidado['fecha'],
                                                       errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    df_novedades_consolidado['redes'] = df_novedades_consolidado['territorio'].map(
        df_distribucion_redes.set_index('TERRITORIO')['RED']
    ).fillna('')

    #df_novedades_consolidado.to_csv(
        #F'../reportes/{FE_REPORTE}/looker/cosolidado_novedades_{FE_REPORTE}.csv')

    # Convertir Timestamps a string

    df_novedades_consolidado['reporte_fecha'] = FE_REPORTE

    df_novedades_consolidado = limpiar_formatos(df_novedades_consolidado, columnas_fecha=['fecha', 'reporte_fecha'])

    cargar_csv_a_bigquery(df_novedades_consolidado, table_id="datos_aps.novedades", project_id="aps-project-478903")

    sobrescribir_hoja(sheet_id, "cosolidado_novedades", df_novedades_consolidado,
                    client)