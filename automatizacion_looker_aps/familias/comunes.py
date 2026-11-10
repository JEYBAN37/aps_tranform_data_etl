"""Funciones compartidas por los ETL (personas, familias, ...).

Ubicación sugerida: automatizacion_looker_aps/utils/comunes.py
"""
from typing import Callable, Optional

import pandas as pd

from mysql_conector import ejecutar_consulta_mysql


def extraer_por_bases(
    cursor,
    bases: list[str],
    query_fn: Callable[[str], str],
    nombre: str,
    columna_origen: Optional[str] = None,
) -> pd.DataFrame:
    """Ejecuta query_fn(base) en cada base y concatena los resultados."""
    frames = []
    for base in bases:
        filas = ejecutar_consulta_mysql(query_fn(base), cursor)
        if not filas:
            print(f"[extract] Sin registros de {nombre} en {base}")
            continue
        columnas = [d[0] for d in cursor.description]
        df = pd.DataFrame(filas, columns=columnas)
        if columna_origen:
            df[columna_origen] = base
        frames.append(df)

    if not frames:
        raise ValueError(f"Extract: ninguna base devolvió {nombre}")

    df = pd.concat(frames, ignore_index=True)
    print(f"[extract] {len(df)} filas de {nombre} ({len(frames)} bases)")
    return df


def limpiar_id(s: pd.Series) -> pd.Series:
    """'123.0' -> '123', nulos -> ''."""
    return (s.fillna("").astype(str).str.strip()
             .str.replace(r"\.0+$", "", regex=True))


def enriquecer_redes(df: pd.DataFrame, df_redes: pd.DataFrame) -> pd.DataFrame:
    """Agrega la columna 'redes' según el territorio (tolera territorios repetidos)."""
    df = df.copy()
    mapa = df_redes.drop_duplicates("TERRITORIO").set_index("TERRITORIO")["RED"]
    df["redes"] = df["territorio"].map(mapa).fillna("")
    return df