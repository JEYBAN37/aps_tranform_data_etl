import pandas as pd
import numpy as np
from datetime import datetime
import time


def sobrescribir_hoja(sheet_id, sheet_name, df, client):
    # 1. Copia y limpieza inicial
    df = df.copy()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    sheet.clear()
    sheet.resize(rows=100000, cols=26)

    # 2. Helper ultra-robusto para convertir celdas a strings
    def _cell_to_string(x):
        # Captura None, NaN y el error de NaT (Not a Time)
        if pd.isna(x):
            return ""

        # Manejo de fechas y tiempos
        if isinstance(x, (pd.Timestamp, datetime, np.datetime64)):
            try:
                ts = pd.to_datetime(x)
                # Si después de convertir sigue siendo NaT, devolver vacío
                if pd.isna(ts):
                    return ""
                return ts.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return str(x)

        # Cualquier otro tipo de dato
        return str(x)

    # 3. Preparación de datos (mucho más rápido que iterrows)
    headers = [str(c) for c in df.columns.tolist()]
    # Convertimos todo el contenido a una lista de listas de strings
    rows = [[_cell_to_string(v) for v in row] for row in df.values]
    all_data = [headers] + rows

    # 4. Carga por bloques (Chunks) para evitar el Error 500 de Google
    chunk_size = 2000  # Ajusta a 2000 si tienes muchísimas columnas
    start_row = 1

    print(f"Subiendo {len(all_data)} filas a la hoja '{sheet_name}'...")

    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i: i + chunk_size]

        # Rango dinámico (ej: A1, A5001, A10001...)
        rango = f"A{start_row}"

        intentos = 3
        for intento in range(intentos):
            try:
                # Actualizar el bloque
                sheet.update(values=chunk, range_name=rango)
                break
            except Exception as e:
                if "500" in str(e) and intento < intentos - 1:
                    print(f"⚠️ Error 500 detectado. Reintentando bloque en {rango}...")
                    time.sleep(10)  # Pausa de seguridad
                else:
                    print(f"❌ Error crítico en fila {start_row}: {e}")
                    raise e

        start_row += len(chunk)

    print(f"✅ Hoja '{sheet_name}' actualizada con éxito.")