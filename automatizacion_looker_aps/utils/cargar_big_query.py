import pandas as pd
import pandas_gbq # Cambiamos to_gbq por la librería directa
from google.api_core import exceptions
import time

def cargar_csv_a_bigquery(df, table_id, project_id):
    # 5. Intento de carga con Reintento (Retry) para mitigar el Error 500
    intentos = 3
    for i in range(intentos):
        try:
            pandas_gbq.to_gbq(
                df,
                destination_table=table_id,
                project_id=project_id,
                if_exists="replace",
                progress_bar=True
            )
            print(f"✅ Carga exitosa en la tabla: {table_id}")
            break
        except Exception as e:
            if "500" in str(e) and i < intentos - 1:
                print(f"⚠️ Error 500 detectado. Reintentando ({i+1}/{intentos})...")
                time.sleep(5) # Esperar 5 segundos antes de reintentar
            else:
                print(f"❌ Error definitivo al cargar: {e}")
                raise e


def limpiar_formatos(df, columnas_fecha=None):
    df = df.copy()
    # 1. Normalizar nombres: BigQuery no acepta espacios ni caracteres especiales en los nombres de columnas
    df.columns = [col.strip().replace(' ', '_').replace('.', '_') for col in df.columns.astype(str)]

    # 2. Quitar duplicados y resetear índice
    df = df.loc[:, ~df.columns.duplicated()]
    df.reset_index(drop=True, inplace=True)

    # 3. Manejo robusto de fechas
    if columnas_fecha:
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # 4. Limpieza de Nulos y Objetos (Aquí suele estar el Error 500)
    for col in df.columns:
        if df[col].dtype == 'object':
            # Reemplazar valores que parecen nulos por None real para que BigQuery lo entienda
            df[col] = df[col].astype(str).replace(['nan', 'None', 'NAT', 'NaN'], None)
        elif pd.api.types.is_float_dtype(df[col]):
            # BigQuery a veces falla con floats que tienen valores infinitos
            df[col] = df[col].replace([float('inf'), float('-inf')], None)

    return df