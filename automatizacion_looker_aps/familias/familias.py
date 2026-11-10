import pandas as pd

from automatizacion_looker_aps.familias.comunes import extraer_por_bases, limpiar_id, enriquecer_redes
from automatizacion_looker_aps.query.query_familia import query_familia
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from export_aps_124 import limpiar_formato_longitud, limpiar_formato_latitud
from mysql_conector import ejecutar_consulta_mysql

"""ETL de familias.

Ubicación sugerida: automatizacion_looker_aps/familias/familias.py
Ajusta los imports marcados con  # AJUSTAR  a la ruta real de tu proyecto.
"""
import os

import pandas as pd

from automatizacion_looker_aps.query.query_familia import query_familia  # AJUSTAR
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja


TABLA_BQ = "datos_aps.familias"
PROYECTO_BQ = "aps-project-478903"
HOJA_SHEETS = "cosolidado_familias"

# columnas de selección múltiple: la coma se cambia por '$' y los nulos por 0
COLS_LISTA = ["menoresriegosalud", "riesgovulnerabilidad", "fortalezas",
              "entornoafectado", "indicadorria"]

COLS_ID = ["responsable_numero", "resposable_plancuidado_id"]

# columnas de texto a las que NO se les quitan signos de puntuación
COLS_SIN_LIMPIAR_TEXTO = {
    "longitud", "latitud", "familiograma", "plancuidado",
    "involucrado_plan_cuidado", "actividaddesarrollar", *COLS_LISTA,
}

# columnas que viajan solo a la tabla de situaciones prioritarias
COLS_SITUACIONES = ["familia_id", "sociambiental_id", "actividaddesarrollar",
                    "involucrado_plan_cuidado", "observacion_id"]
COLS_SOLO_SITUACIONES = ["actividaddesarrollar", "observacion_id"]

# reglas de validación (si hay varias, gana la última de la lista)
ERROR_COORDENADAS = "ERROR EN CARACTERIZACION (COORDENADAS INVALIDAS)"
ERROR_SIN_INTEGRANTES = "ERROR EN CARACTERIZACION (SIN INTEGRANTES)"

COLUMNAS_REQUERIDAS = [
    "familia_id", "db", "sociambiental_id", "territorio", "fecha",
    "longitud", "latitud", "total_personas_cursos_vida",
    *COLS_ID, *COLS_LISTA, *COLS_SITUACIONES,
]


# ───────────────────────── 2. EXTRACT ─────────────────────────
def extraer_familias(cursor, bases: list[str]) -> pd.DataFrame:
    return extraer_por_bases(cursor, bases, query_familia, "familias")


# ───────────────────────── 3. TRANSFORM ─────────────────────────
def deduplicar_familias(df):
    return df.drop_duplicates(subset=["familia_id", "db"], keep="first").copy()


def limpiar_coordenadas(df):
    df = df.copy()
    df["longitud"] = df["longitud"].apply(limpiar_formato_longitud)
    df["latitud"] = df["latitud"].apply(limpiar_formato_latitud)
    return df


def separar_listas(df):
    """Cambia ',' por '$' en las columnas de selección múltiple y rellena nulos con 0."""
    df = df.copy()
    for c in COLS_LISTA:
        s = df[c].astype(object)
        es_texto = s.map(lambda v: isinstance(v, str))
        df[c] = s.mask(es_texto, s.str.replace(",", "$", regex=False)).fillna(0)
    return df


def limpiar_texto(df):
    df = df.copy()
    cols = [c for c in df.select_dtypes(include="object").columns
            if c not in COLS_SIN_LIMPIAR_TEXTO]
    for c in cols:
        df[c] = (df[c].fillna("").astype(str).str.strip()
                   .str.replace(r"[^\w\s]", "", regex=True))
    return df


def procesar_fecha(df):
    df = df.copy()
    df["fecha"] = (pd.to_datetime(df["fecha"], errors="coerce")
                     .dt.strftime("%Y-%m-%d").fillna(""))
    return df


def limpiar_ids(df):
    df = df.copy()
    for c in COLS_ID:
        df[c] = limpiar_id(df[c])
    return df


def marcar_errores_caracterizacion(df):
    df = df.copy()
    df["validacion"] = ""

    sin_coordenadas = df["longitud"].isna() | df["latitud"].isna()
    df.loc[sin_coordenadas, "validacion"] = ERROR_COORDENADAS

    total = pd.to_numeric(df["total_personas_cursos_vida"], errors="coerce")
    df.loc[total == 0, "validacion"] = ERROR_SIN_INTEGRANTES   # pisa al anterior
    return df


def transformar_familias(df, df_redes, fe_reporte):
    df = (df
          .pipe(deduplicar_familias)
          .pipe(limpiar_coordenadas)
          .pipe(separar_listas)
          .pipe(limpiar_texto)
          .pipe(procesar_fecha)
          .pipe(limpiar_ids)
          .pipe(marcar_errores_caracterizacion)
          .pipe(enriquecer_redes, df_redes))
    df["reporte_fecha"] = fe_reporte
    return df


# ───────────────────────── 4. VALIDAR ─────────────────────────
def validar_familias(df: pd.DataFrame) -> pd.DataFrame:
    faltantes = set(COLUMNAS_REQUERIDAS) - set(df.columns)
    assert not faltantes, f"Faltan columnas: {faltantes}"
    assert len(df) > 0, "DataFrame vacío"
    assert df["familia_id"].notna().all(), "Hay filas sin familia_id"
    assert not df.duplicated(subset=["familia_id", "db"]).any(), "familia_id+db duplicados"

    for error, n in df["validacion"].value_counts().items():
        if error:
            print(f"[calidad] ⚠ {n} familias con: {error}")
    sin_red = (df["redes"] == "").sum()
    if sin_red:
        print(f"[calidad] ⚠ {sin_red} familias sin red asignada")
    return df


# ───────────────────────── 5. PREPARAR SALIDAS ─────────────────────────
def formatear_para_carga(df):
    return limpiar_formatos(df, columnas_fecha=["fecha", "reporte_fecha"])


def extraer_situaciones_prioritarias(df):
    return df[COLS_SITUACIONES].copy()


def preparar_familias_final(df):
    return df.drop(columns=COLS_SOLO_SITUACIONES)


# ───────────────────────── 6. LOAD ─────────────────────────
def cargar_csv_familias(df, fe_reporte):
    ruta = f"../reportes/{fe_reporte}/looker/cosolidado_familias_{fe_reporte}.csv"
    df.to_csv(ruta, index=False)


def cargar_bigquery_familias(df):
    cargar_csv_a_bigquery(df, table_id=TABLA_BQ, project_id=PROYECTO_BQ)


def cargar_sheets_familias(df, sheet_id, client):
    sobrescribir_hoja(sheet_id, HOJA_SHEETS, df, client)


# ───────────────────────── 7. ORQUESTACIÓN (sin Dagster) ─────────────────────────
def etl_familias(cursor, df_redes, fe_reporte, client, bases, sheet_id):
    crudo = extraer_familias(cursor, bases)
    limpio = transformar_familias(crudo, df_redes, fe_reporte)
    validado = validar_familias(limpio)

    cargar_csv_familias(validado, fe_reporte)

    formateado = formatear_para_carga(validado)
    situaciones = extraer_situaciones_prioritarias(formateado)
    familias = preparar_familias_final(formateado)

    cargar_bigquery_familias(familias)
    cargar_sheets_familias(familias, sheet_id, client)
    return familias, situaciones