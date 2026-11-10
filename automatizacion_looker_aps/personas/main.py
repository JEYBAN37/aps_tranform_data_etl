import os
import sys
from contextlib import closing
from datetime import datetime

import gspread
import mysql.connector
from google.oauth2.service_account import Credentials

from automatizacion_looker_aps.personas.personas import etl_personas
from credenciales import DATABASE, DATABASE_APS2025, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, MYSQL_APS
from limpieza_datos import extraer_distribucion_redes

# ───────────── CONFIG (todo en un lugar) ─────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
RUTA_CREDS = "credentials.json"
SHEET_ID_PERSONAS = os.getenv("SHEET_ID_PERSONAS", "1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")


# ───────────── CONEXIONES ─────────────
def conectar_mysql():
    return mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=True,          # solo lecturas: no hay nada que confirmar
    )


def conectar_sheets():
    """Falla rápido y con mensaje claro si algo está mal."""
    if not os.path.exists(RUTA_CREDS):
        raise FileNotFoundError(f"No existe '{RUTA_CREDS}' en {os.getcwd()}")

    creds = Credentials.from_service_account_file(RUTA_CREDS, scopes=SCOPES)
    client = gspread.authorize(creds)
    client.list_spreadsheet_files()   # prueba real de conexión
    print("✅ Conexión a Google verificada")
    return client


# ───────────── PRE-CHEQUEOS ─────────────
def preparar_entorno(fe_reporte: str):
    os.makedirs(f"../reportes/{fe_reporte}/looker", exist_ok=True)



# ───────────── MAIN ─────────────
def main():
    fe_reporte = datetime.now().strftime("%Y-%m-%d")
    print(f"▶ Inicio ETL personas | reporte {fe_reporte}")

    # 1. Validar todo lo externo ANTES de extraer
    client = conectar_sheets()
    preparar_entorno(fe_reporte)

    # 2. Datos de apoyo
    df_redes = extraer_distribucion_redes()

    # 3. Pipeline principal (la conexión se cierra sola, pase lo que pase)
    with closing(conectar_mysql()) as connection, closing(connection.cursor()) as cursor:
        df_personas = etl_personas(
            cursor=cursor,
            df_redes=df_redes,
            fe_reporte=fe_reporte,
            client=client,
            bases=DATABASE,
            sheet_id=SHEET_ID_PERSONAS,
        )

    print(f"✔ ETL terminado: {len(df_personas)} personas")
    return df_personas


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"✖ ETL falló: {type(e).__name__}: {e}")
        sys.exit(1)      # código de salida != 0 para que un scheduler detecte el fallo