import os
import sys
from contextlib import closing, contextmanager
from datetime import datetime

import gspread
import mysql.connector
from google.oauth2.service_account import Credentials

from automatizacion_looker_aps.familias.familias import etl_familias
from automatizacion_looker_aps.intervenciones.filtrado_actividades import etl_actividades
from automatizacion_looker_aps.intervenciones.verificar_intervenciones import etl_verificar_indicadores
from automatizacion_looker_aps.novedades.novedades import etl_novedades
from automatizacion_looker_aps.personas.personas import etl_personas
from automatizacion_looker_aps.responsables.responsable import extraer_responsables
from credenciales import DATABASE, DATABASE_APS2025, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, MYSQL_APS
from limpieza_datos import extraer_distribucion_redes

# ───────────── CONFIG (todo en un lugar) ─────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
RUTA_CREDS = "credentials.json"
SHEET_ID_PERSONAS = os.getenv("SHEET_ID_PERSONAS", "1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
SHEET_ID_FAMILIAS = os.getenv("SHEET_ID_FAMILIAS", "1HbJo2ZINdgZshcAIj7I1u-azaXPZHd1KbNdsdTASQGI")
SHEET_ID_NOVEDADES = os.getenv("SHEET_ID_NOVEDADES", "1Yr9gvmWQ7i6ANgI-9LAW8Yfwi6HScJfF7ll3nm0StTE")
SHEET_ID_ACTIVIDADES = os.getenv("SHEET_ID_ACTIVIDADES", "14NIa4AlbXU5pVXmhLBbJwnidy4HmI2ZatFfMJ91ThXw")
SHEET_ID_INDICADORES = os.getenv("SHEET_ID_INDICADORES", "1_jymVqZ5GocxJZTtoBZky94O0B5JoXa95Cn3lGw7scI")


# ───────────── CONEXIONES ─────────────
def conectar_mysql():
    return mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=True,          # solo lecturas: no hay nada que confirmar
    )


@contextmanager
def cursor_mysql():
    """Conexión + cursor nuevos para cada paso. Se cierran siempre, y una conexión
    que lleva mucho rato abierta (mientras se cargan BigQuery y Sheets) no se cae."""
    with closing(conectar_mysql()) as connection, closing(connection.cursor()) as cursor:
        yield cursor


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


# ───────────── EJECUCIÓN CONTROLADA ─────────────
def ejecutar(nombre, paso, resultados, fallos):
    """Corre un paso; si falla lo registra y deja que los demás continúen."""
    try:
        res = paso()
        resultados[nombre] = res
        principal = res[0] if isinstance(res, tuple) else res
        filas = f": {len(principal)} filas" if hasattr(principal, "__len__") else ""
        print(f"✔ {nombre} terminado{filas}")
    except Exception as e:
        fallos[nombre] = e
        print(f"✖ {nombre} falló: {type(e).__name__}: {e}")


def requiere(nombre, dependencias, resultados, fallos):
    """True si todo lo que necesita el paso ya está. Si no, lo omite y lo deja anotado."""
    faltan = [d for d in dependencias if d not in resultados]
    if faltan:
        fallos[nombre] = RuntimeError(f"omitido: faltan {', '.join(faltan)}")
        print(f"⏭ {nombre} omitido: faltan {', '.join(faltan)}")
        return False
    return True


# ───────────── MAIN ─────────────
def main():
    fe_reporte = datetime.now().strftime("%Y-%m-%d")
    print(f"▶ Inicio ETL | reporte {fe_reporte}")

    # 1. Validar todo lo externo ANTES de extraer
    client = conectar_sheets()
    preparar_entorno(fe_reporte)

    # 2. Datos de apoyo (los usan varios ETL)
    df_redes = extraer_distribucion_redes()

    resultados, fallos = {}, {}

    # 3. ETLs independientes entre sí: si uno falla, los demás siguen
    independientes = (
        ("personas", etl_personas, SHEET_ID_PERSONAS),
        ("familias", etl_familias, SHEET_ID_FAMILIAS),
        ("novedades", etl_novedades, SHEET_ID_NOVEDADES),
    )
    for nombre, etl, sheet_id in independientes:
        def paso(etl=etl, sheet_id=sheet_id):
            with cursor_mysql() as cursor:
                return etl(cursor=cursor, df_redes=df_redes, fe_reporte=fe_reporte,
                           client=client, bases=DATABASE, sheet_id=sheet_id)
        ejecutar(nombre, paso, resultados, fallos)

    # 4. Responsables (insumo de indicadores)
    def paso_responsables():
        with cursor_mysql() as cursor:
            return extraer_responsables(cursor, DATABASE)
    ejecutar("responsables", paso_responsables, resultados, fallos)

    # 5. ETLs que dependen de los anteriores
    if requiere("indicadores", ["familias", "responsables"], resultados, fallos):
        df_familias, df_situaciones = resultados["familias"]
        ejecutar("indicadores", lambda: etl_verificar_indicadores(
            df_intervenciones=df_situaciones,
            sheet_id=SHEET_ID_INDICADORES,
            client=client,
            df_responsables=resultados["responsables"],
            df_familia=df_familias,
        ), resultados, fallos)

    if requiere("actividades", ["familias", "personas"], resultados, fallos):
        df_familias = resultados["familias"][0]

        def paso_actividades():
            with cursor_mysql() as cursor:
                return etl_actividades(
                    cursor=cursor,
                    df_familia=df_familias,
                    df_personas=resultados["personas"],
                    fe_reporte=fe_reporte,
                    client=client,
                    bases=DATABASE,
                    sheet_id=SHEET_ID_ACTIVIDADES,
                )
        ejecutar("actividades", paso_actividades, resultados, fallos)

    if fallos:
        raise RuntimeError(f"ETL con fallos: {', '.join(fallos)}")
    return resultados


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"✖ ETL falló: {type(e).__name__}: {e}")
        sys.exit(1)      # código de salida != 0 para que un scheduler detecte el fallo