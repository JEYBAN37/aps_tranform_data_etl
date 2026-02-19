import pandas as pd

from automatizacion_looker_aps.query.query_persona import query_persona
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from mysql_conector import ejecutar_consulta_mysql


def cargar_personas(cursor, df_distribucion_redes,FE_REPORTE,client,db,sheet_id):
    acumulado_personas = []
    for database in db:
        personas_query = ejecutar_consulta_mysql(query_persona(database), cursor)
        if not personas_query:
            print(f"No se encontraron familias para {database}. Continuando.")
            continue
        acumulado_personas.extend(personas_query)

    personas = acumulado_personas
    df_personas_consolidados = pd.DataFrame(personas)
    df_personas_consolidados.columns = [desc[0] for desc in cursor.description]

    df_personas_consolidados['familia_id'] = (
        df_personas_consolidados['familia_id']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )

    df_personas_consolidados['id'] = (
        df_personas_consolidados['familia_id']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )


    df_personas_consolidados['sociambiental_id'] = (
        df_personas_consolidados['sociambiental_id']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )

    df_personas_consolidados['numero'] = (
        df_personas_consolidados['numero']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )

    for i, col in enumerate(df_personas_consolidados.columns):
        if df_personas_consolidados.dtypes.iloc[i] == object and col not in ('canalizacionuno'):
            df_personas_consolidados[col] = df_personas_consolidados[col].fillna('').astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

    df_personas_consolidados['canalizacionuno'] = (
        df_personas_consolidados['canalizacionuno']
        .str.replace(',', '$', regex=True)
    )

    df_personas_consolidados['fecha'] = pd.to_datetime(df_personas_consolidados['fecha'],
                                                       errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    df_personas_consolidados['fechanac'] = pd.to_datetime(df_personas_consolidados['fechanac'], errors='coerce')
    today = pd.to_datetime('today').normalize()

    def _calc_age(birth):
        if pd.isna(birth):
            return ''
        return str(today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day)))

    df_personas_consolidados['edad'] = df_personas_consolidados['fechanac'].apply(_calc_age)
    df_personas_consolidados['fechanac'] = df_personas_consolidados['fechanac'].dt.strftime('%Y-%m-%d').fillna(
        '').astype(str)

    df_personas_consolidados['doc_id'] = df_personas_consolidados['doc_id'].astype(str).str.strip().str.replace(
        r'\D+', '', regex=True)

    df_personas_consolidados['redes'] = df_personas_consolidados['territorio'].map(
        df_distribucion_redes.set_index('TERRITORIO')['RED']
    ).fillna('')

    # quiero contar cuantos registros hay por estado
    total_rows = len(df_personas_consolidados)

    df_personas_consolidados.to_csv(
        F'../reportes/{FE_REPORTE}/looker/cosolidado_personas_{FE_REPORTE}.csv')

    #df_personas_consolidados.to_csv(F'../crucez/base_actualizada/cosolidado_personas_{FE_REPORTE}.csv', index=False)

    df_personas_consolidados['reporte_fecha'] = FE_REPORTE

    #cargar_csv_a_bigquery(df_personas_consolidados, table_id="datos_aps.personas", project_id="aps-project-478903",
                          #columnas_fecha=['fechanac', 'fecha', 'reporte_fecha'])

    #sobrescribir_hoja(sheet_id, "cosolidado_personas", df_personas_consolidados,
                    #client)

    return df_personas_consolidados