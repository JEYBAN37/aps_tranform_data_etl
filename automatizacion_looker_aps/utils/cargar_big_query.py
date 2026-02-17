import pandas as pd
from pandas.io.gbq import to_gbq
def cargar_csv_a_bigquery(df, table_id, project_id, columnas_fecha=None):
    df = df.copy()

    # Normalizar nombres de columnas
    df.columns = df.columns.astype(str)

    # Quitar columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    # Resetear índice (CRÍTICO)
    df.reset_index(drop=True, inplace=True)

    # Convertir columnas de fecha explícitamente
    if columnas_fecha:
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convertir SOLO objetos que no sean fecha
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="replace"
    )