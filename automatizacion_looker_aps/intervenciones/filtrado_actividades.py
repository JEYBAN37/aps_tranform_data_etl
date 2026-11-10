"""ETL de actividades (intervenciones).

Ubicación sugerida: automatizacion_looker_aps/actividades/actividades.py

Depende de los resultados de personas y familias (df_personas, df_familia):
se usan para clasificar cada intervención (nueva caracterización, actualización
de ficha, plan de cuidado creado o firmado).
"""
import ast
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd

from automatizacion_looker_aps.familias.comunes import extraer_por_bases
from automatizacion_looker_aps.query.query_intervenciones import query_intervenciones
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja


# ───────────────────────── 1. REGLAS DE DATOS (config) ─────────────────────────
HOJA_SHEETS = "ACTIVIDADES"
TABLA_BQ = "datos_aps.actividades"
PROYECTO_BQ = "aps-project-478903"

COLS_ID = ["observacion_id", "familia_id", "sociambiental_id", "juventudadultos_id"]
COLS_SIN_LIMPIAR_TEXTO = {"historial", "fecha"}

COLS_AGRUPACION = [
    "responsable_id", "fecha", "observacion_id", "familia_id", "sociambiental_id",
    "juventudadultos_id", "responsable_nombre", "responsable_profesion",
    "conteo_nuevas_caracterizaciones", "conteo_plan_cuidado", "id_familia_plan",
    "conteo_actualizaciones_ficha",
]

ERROR_CARACTERIZACION = "ERROR EN CARACTERIZACION"
PLAN_CREADO = "PLAN DE CUIDADO CREADO"
PLAN_FIRMADO = "PLAN DE CUIDADO FIRMADO"
PLAN_ERROR = "PLAN DE CUIDADO NO VALIDO ERROR"

# Una intervención cuenta como nueva caracterización solo si es posterior a esta fecha
FECHA_INICIO_CARACTERIZACIONES = "2025-12-31"
# Una intervención sobre una persona cuenta como actualización si pasan más de N días
# desde la caracterización de su familia
DIAS_ACTUALIZACION_PERSONA = 30

COLS_CRUDO_REQUERIDAS = [*COLS_ID, "historial", "fecha", "responsable_id",
                         "responsable_nombre", "responsable_profesion"]
COLS_FAMILIA_REQUERIDAS = ["familia_id", "sociambiental_id", "fecha", "validacion", "db",
                           "dirfamiliograma", "familiograma", "dirplancuidado", "plancuidado"]
COLS_PERSONAS_REQUERIDAS = ["juventud_id", "familia_id", "doc_id"]
COLUMNAS_SALIDA = [*COLS_AGRUPACION, "count", "dir_anexo", "anexo", "db_anterior"]


# ───────────────────────── 2. EXTRACT ─────────────────────────
def extraer_actividades(cursor, bases: list[str]) -> pd.DataFrame:
    return extraer_por_bases(cursor, bases, query_intervenciones, "intervenciones")


# ───────────────────────── 3. UTILIDADES ─────────────────────────
_NULOS = {"", "nan", "none", "<na>", "nat"}


def clave(valor):
    """Normaliza un id para compararlo: 123, 123.0 y '123' -> '123'. Nulos -> None."""
    if valor is None:
        return None
    texto = re.sub(r"\.0+$", "", str(valor).strip())
    return None if texto.lower() in _NULOS else texto


def parse_historial(valor) -> dict:
    """Convierte el campo 'historial' (JSON o dict en texto) en un dict. Una sola vez por fila."""
    if isinstance(valor, dict):
        return valor
    if isinstance(valor, str):
        try:
            res = json.loads(valor)
        except Exception:
            try:
                res = ast.literal_eval(valor)
            except Exception:
                return {}
        return res if isinstance(res, dict) else {}
    return {}


# ───────────────────────── 4. LIMPIEZA BÁSICA ─────────────────────────
def limpiar_ids(df):
    """Ids como texto sin '.0'. Los nulos quedan como 'nan' (igual que antes)."""
    df = df.copy()
    for c in COLS_ID:
        df[c] = (df[c].fillna("nan").astype(str).str.strip()
                   .str.replace(r"\.0+$", "", regex=True))
    return df


def limpiar_texto(df):
    df = df.copy()
    excluir = set(COLS_ID) | COLS_SIN_LIMPIAR_TEXTO
    for c in df.select_dtypes(include="object").columns:
        if c in excluir:
            continue
        df[c] = (df[c].fillna("").astype(str).str.strip()
                   .str.replace(r"[^\w\s]", "", regex=True))
    return df


def procesar_fecha(df):
    df = df.copy()
    df["fecha"] = (pd.to_datetime(df["fecha"], errors="coerce")
                     .dt.strftime("%Y-%m-%d").fillna(""))
    return df


# ───────────────────────── 5. CONTEXTO (búsquedas precalculadas) ─────────────────────────
@dataclass
class Contexto:
    familias: dict               # familia_id -> {fecha, hay_error, validacion, id}
    sociambiental: dict          # sociambiental_id -> {total, ok, hay_error, validacion}
    personas: dict               # juventud_id -> {familia_id, doc_id}
    tabla_familias: pd.DataFrame  # una fila por familia (para copiar atributos)


def _faltantes(df, requeridas, nombre):
    faltan = set(requeridas) - set(df.columns)
    if faltan:
        raise ValueError(f"{nombre}: faltan columnas {sorted(faltan)}")


def validar_insumos(df_crudo, df_familia, df_personas):
    _faltantes(df_crudo, COLS_CRUDO_REQUERIDAS, "actividades")
    _faltantes(df_familia, COLS_FAMILIA_REQUERIDAS, "familias")
    _faltantes(df_personas, COLS_PERSONAS_REQUERIDAS, "personas")


def construir_contexto(df_familia: pd.DataFrame, df_personas: pd.DataFrame) -> Contexto:
    """Antes se filtraba el DataFrame de familias por cada fila (muy lento).
    Ahora se agrupa una sola vez y las reglas consultan diccionarios."""
    fam = df_familia.copy()
    fam["_fid"] = fam["familia_id"].map(clave)
    fam["_sid"] = fam["sociambiental_id"].map(clave)
    fam["fecha"] = (pd.to_datetime(fam["fecha"], errors="coerce")
                      .dt.strftime("%Y-%m-%d").fillna(""))
    fam["_error"] = fam["validacion"].astype(str).str.contains(ERROR_CARACTERIZACION, na=False)
    fam["_ok"] = ~fam["_error"]
    col_id = "id" if "id" in fam.columns else "familia_id"

    con_familia = fam.dropna(subset=["_fid"])
    familias = (con_familia.groupby("_fid", sort=False)
                .agg(fecha=("fecha", "first"), hay_error=("_error", "any"),
                     validacion=("validacion", "first"), id=(col_id, "first"))
                .to_dict("index"))

    viviendas = (fam.dropna(subset=["_sid"]).groupby("_sid", sort=False)
                 .agg(total=("_ok", "size"), ok=("_ok", "sum"),
                      hay_error=("_error", "any"), validacion=("validacion", "first"))
                 .to_dict("index"))

    per = df_personas.copy()
    per["_jid"] = per["juventud_id"].map(clave)
    per["_fid"] = per["familia_id"].map(clave)
    personas = (per.dropna(subset=["_jid"]).drop_duplicates("_jid")
                .set_index("_jid")[["_fid", "doc_id"]]
                .rename(columns={"_fid": "familia_id"})
                .to_dict("index"))

    tabla = con_familia.drop_duplicates("_fid").set_index("_fid")
    return Contexto(familias, viviendas, personas, tabla)


def atributo_familia(ids: pd.Series, ctx: Contexto, columna: str) -> pd.Series:
    """Valor de `columna` para la familia de cada fila; '0' si la familia no existe."""
    mapa = ctx.tabla_familias[columna]
    claves = ids.map(clave)
    return claves.map(mapa).where(claves.isin(mapa.index), "0")


# ───────────────────────── 6. REGLAS DE NEGOCIO (una por tipo de conteo) ─────────────────────────
# Los textos de salida son idénticos a la versión anterior (incluidos espacios y
# sufijos |C, | O, |O) para no romper los cálculos de Looker.

def _texto_ficha(fam, texto_ok, sufijo):
    if not fam["hay_error"]:
        return f" 1 | {texto_ok} {sufijo}"
    return f" 1 | {str(fam['validacion']).strip()} {sufijo}"


def regla_nueva_caracterizacion(fila, hist, ctx):
    sid = clave(fila.get("sociambiental_id"))
    vivienda = ctx.sociambiental.get(sid) if sid else None
    if vivienda is None or hist.get("updateDate"):
        return "0"
    if (fila.get("fecha") or "") <= FECHA_INICIO_CARACTERIZACIONES:
        return "0"
    if int(vivienda["ok"]) > 0:
        return f" {int(vivienda['ok'])} | OK"
    return f" {int(vivienda['total'])} | {vivienda['validacion']} "


def regla_actualizacion_ficha(fila, hist, ctx):
    sid = clave(fila.get("sociambiental_id"))
    fid = clave(fila.get("familia_id"))
    jid = clave(fila.get("juventudadultos_id"))
    obs = clave(fila.get("observacion_id"))

    fecha_fila = fila.get("fecha") or "1900-01-01"
    fecha_modificacion = hist.get("fecha") or "1900-01-01"
    hubo_actualizacion = bool(hist.get("updateDate"))
    # (la clave del JSON viene escrita 'dirfamilliograma', con doble l)
    dir_familiograma = hist.get("dirfamilliograma") or None

    # A. Ficha de vivienda
    if sid:
        vivienda = ctx.sociambiental.get(sid)
        if vivienda is None:
            return " 1 | VERIFICAR FAMILIA NO TIENE |C"
        if hubo_actualizacion and fecha_fila > fecha_modificacion:
            return _texto_ficha(vivienda, "ACTUALIZACION DE FICHA VIVIENDA", "|C")

    # B. Ficha de familia
    if fid:
        fam = ctx.familias.get(fid)
        if fam is None:
            return " 1 | VERIFICAR FAMILIA NO TIENE |C"
        if (fila.get("fecha") or "") > fam["fecha"]:
            return _texto_ficha(fam, "ACTUALIZACION DE FICHA FAMILIA", "|C")

    # C. Ficha de persona (más de N días después de la caracterización de su familia)
    if jid:
        persona = ctx.personas.get(jid)
        if persona is None:
            return " 1 | VERIFICAR PERSONA NO EXISTE |C"
        fam = ctx.familias.get(persona["familia_id"])
        if fam is None:
            return " 1 | VERIFICAR FAMILIA NO TIENE  |C"
        limite = pd.to_datetime(fam["fecha"], errors="coerce") + pd.Timedelta(days=DIAS_ACTUALIZACION_PERSONA)
        if pd.to_datetime(fecha_fila, errors="coerce") > limite:
            return _texto_ficha(fam, f"ACTUALIZACION DE FICHA PERSONA {persona['doc_id']}", "| O")

    # D. Observación sobre una familia
    if obs and fid:
        fam = ctx.familias.get(fid)
        if fam is None:
            return " 1 | VERIFICAR FAMILIA NO TIENE  |O"
        if dir_familiograma is not None:
            return _texto_ficha(fam, f"ACTUALIZACION DE OBSERVACION {fam['id']}", "|O")

    return "0"


def regla_plan_cuidado(fila, hist, ctx):
    """Devuelve 'TIPO DE PLAN | familia_id' (se separa después en dos columnas)."""
    if not clave(fila.get("observacion_id")) or hist.get("dirfamilliograma"):
        return "0"

    fid = clave(hist.get("familia_id"))
    if fid is None:
        return PLAN_ERROR

    fam = ctx.familias.get(fid)
    if fam is None:
        return f"PLAN DE CUIDADO NO VALIDO | {fid}"
    if fam["hay_error"]:
        return f"PLAN DE CUIDADO CON {str(fam['validacion']).strip()} | {fid}"
    if hist.get("plancuidado"):
        return f"{PLAN_FIRMADO} | {fid}"

    actividad = hist.get("actividaddesarrollar")
    if actividad:
        return f"{PLAN_CREADO} | {fid}"
    if actividad == "":
        return f"PLAN DE CUIDADO NO VALIDO | {fid}"
    return "0"


def _evaluar(regla, valor_error, fila, hist, ctx, errores):
    """Ejecuta una regla; si falla devuelve valor_error y lo registra (no se pierde en silencio)."""
    try:
        return regla(fila, hist, ctx)
    except Exception as e:
        errores[regla.__name__].append(f"{type(e).__name__}: {e}")
        return valor_error


def clasificar_intervenciones(df: pd.DataFrame, ctx: Contexto) -> pd.DataFrame:
    errores = defaultdict(list)
    nuevas, fichas, planes = [], [], []

    historiales = df["historial"].map(parse_historial)     # se parsea una sola vez
    for fila, hist in zip(df.to_dict("records"), historiales):
        nuevas.append(_evaluar(regla_nueva_caracterizacion, "0", fila, hist, ctx, errores))
        fichas.append(_evaluar(regla_actualizacion_ficha, "0", fila, hist, ctx, errores))
        planes.append(_evaluar(regla_plan_cuidado, PLAN_ERROR, fila, hist, ctx, errores))

    for nombre, lista in errores.items():
        print(f"[calidad] ⚠ {len(lista)} filas fallaron en {nombre}. Ejemplo: {lista[0]}")

    df = df.copy()
    df["conteo_nuevas_caracterizaciones"] = nuevas
    df["conteo_actualizaciones_ficha"] = fichas
    df["conteo_plan_cuidado"] = planes
    return df


def separar_plan_y_familia(df):
    """'PLAN DE CUIDADO CREADO | 123' -> conteo_plan_cuidado='PLAN DE CUIDADO CREADO', id_familia_plan='123'."""
    df = df.copy()
    conteo = df["conteo_plan_cuidado"]
    partes = conteo.str.split("|", regex=False)
    df["id_familia_plan"] = partes.str[-1].str.strip().where(conteo != "0", "0")
    df["conteo_plan_cuidado"] = partes.str[0].str.strip().where(conteo != "0", "0")
    return df


# ───────────────────────── 7. AGRUPAR Y SELECCIONAR PLANES ─────────────────────────
def agrupar_actividades(df):
    resultado = df.groupby(COLS_AGRUPACION).size().reset_index(name="count")
    perdidas = len(df) - int(resultado["count"].sum())
    if perdidas:
        print(f"[calidad] ⚠ {perdidas} filas descartadas al agrupar (alguna clave nula, p. ej. responsable_id)")
    return resultado.sort_values(["fecha", "responsable_id"])


def seleccionar_planes_creados(df):
    """Un plan creado por familia: el de fecha más antigua."""
    creados = df[df["conteo_plan_cuidado"] == PLAN_CREADO]
    return (creados.sort_values(["fecha", "id_familia_plan"])
                   .drop_duplicates("id_familia_plan", keep="first")
                   .sort_values("id_familia_plan", kind="stable")
                   .reset_index(drop=True))


def seleccionar_planes_firmados(df, df_creados):
    """Solo cuenta un plan firmado si lo firma el mismo responsable que lo creó.
    Una firma por (familia, responsable): la de fecha más antigua."""
    firmados = (df[df["conteo_plan_cuidado"] == PLAN_FIRMADO]
                .sort_values(["id_familia_plan", "fecha", "responsable_id"]))
    claves = df_creados[["id_familia_plan", "responsable_id"]].drop_duplicates()
    firmados = firmados.merge(claves, on=["id_familia_plan", "responsable_id"], how="inner")
    return (firmados.drop_duplicates(["id_familia_plan", "responsable_id"], keep="first")
                    .reset_index(drop=True))


def agregar_anexos(df, ctx, col_dir, col_anexo):
    df = df.copy()
    df["dir_anexo"] = atributo_familia(df["id_familia_plan"], ctx, col_dir)
    df["anexo"] = atributo_familia(df["id_familia_plan"], ctx, col_anexo)
    return df


# ───────────────────────── 8. TRANSFORM (orquestación de los pasos) ─────────────────────────
def transformar_actividades(df_crudo, df_familia, df_personas):
    validar_insumos(df_crudo, df_familia, df_personas)
    ctx = construir_contexto(df_familia, df_personas)

    df = (df_crudo
          .pipe(limpiar_ids)
          .pipe(limpiar_texto)
          .pipe(procesar_fecha)
          .pipe(clasificar_intervenciones, ctx)
          .pipe(separar_plan_y_familia)
          .pipe(agrupar_actividades))

    creados = seleccionar_planes_creados(df)
    firmados = seleccionar_planes_firmados(df, creados)
    otros = df[~df["conteo_plan_cuidado"].isin([PLAN_FIRMADO, PLAN_CREADO])]

    creados = agregar_anexos(creados, ctx, "dirfamiliograma", "familiograma")
    firmados = agregar_anexos(firmados, ctx, "dirplancuidado", "plancuidado")

    final = pd.concat([firmados, creados, otros], ignore_index=True)
    final["db_anterior"] = atributo_familia(final["id_familia_plan"], ctx, "db")
    return final


# ───────────────────────── 9. VALIDAR ─────────────────────────
def validar_actividades(df: pd.DataFrame) -> pd.DataFrame:
    faltantes = set(COLUMNAS_SALIDA) - set(df.columns)
    assert not faltantes, f"Faltan columnas: {faltantes}"
    assert len(df) > 0, "DataFrame vacío"

    creados = df[df["conteo_plan_cuidado"] == PLAN_CREADO]
    assert not creados["id_familia_plan"].duplicated().any(), "Hay familias con más de un plan creado"
    firmados = df[df["conteo_plan_cuidado"] == PLAN_FIRMADO]
    assert not firmados.duplicated(["id_familia_plan", "responsable_id"]).any(), \
        "Hay planes firmados duplicados (familia + responsable)"

    print("[calidad] filas por tipo de plan:\n" + df["conteo_plan_cuidado"].value_counts().to_string())

    planes = df["conteo_plan_cuidado"].isin([PLAN_CREADO, PLAN_FIRMADO])
    sin_familia = (planes & (df["db_anterior"] == "0")).sum()
    if sin_familia:
        print(f"[calidad] ⚠ {sin_familia} planes cuya familia no está en la tabla de familias")
    return df


# ───────────────────────── 10. LOAD ─────────────────────────
def formatear_actividades(df):
    return limpiar_formatos(df, columnas_fecha=["fecha"])


def cargar_csv_actividades(df, fe_reporte):
    ruta = f"../reportes/{fe_reporte}/looker/consolidado_actividades_{fe_reporte}.csv"
    df.to_csv(ruta, index=False)


def cargar_bigquery_actividades(df):
    cargar_csv_a_bigquery(df, table_id=TABLA_BQ, project_id=PROYECTO_BQ)


def cargar_sheets_actividades(df, sheet_id, client):
    if not sheet_id:
        raise ValueError("Falta SHEET_ID_ACTIVIDADES (variable de entorno o constante)")
    sobrescribir_hoja(sheet_id, HOJA_SHEETS, df.drop(columns=COLS_ID), client)   # la hoja no lleva los ids


# ───────────────────────── 11. ORQUESTACIÓN (sin Dagster) ─────────────────────────
def etl_actividades(cursor, df_familia, df_personas, fe_reporte, client, bases,
                    sheet_id):
    crudo = extraer_actividades(cursor, bases)
    transformado = transformar_actividades(crudo, df_familia, df_personas)
    validado = validar_actividades(transformado)
    final = formatear_actividades(validado)

    cargar_csv_actividades(final, fe_reporte)
    cargar_bigquery_actividades(final)
    cargar_sheets_actividades(final, sheet_id, client)
    return final


import ast
import json

import pandas as pd

from automatizacion_looker_aps.query.query_intervenciones import query_intervenciones
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from mysql_conector import ejecutar_consulta_mysql

def limpiar_id (df, column_name):
    df[column_name] = (
        df[column_name]
        .fillna('nan')
        .astype(str)
        .str.strip()
        .str.replace(r'\.0+$', '', regex=True)
    )
    return df


def filtrar_planes_cuidado_creado(df_actividades_consolidados, filtrar_por_tipo='PLAN DE CUIDADO CREADO'):
    df_planes_sin_filtrar = df_actividades_consolidados[
        df_actividades_consolidados['conteo_plan_cuidado'] == filtrar_por_tipo]


    df_planes_sin_filtrar = df_planes_sin_filtrar.sort_values(
        by=['fecha', 'id_familia_plan'],
        ascending=[True, True]
    )

    df_planes_sin_filtrar.drop_duplicates(subset=['fecha', 'id_familia_plan'], keep='first', inplace=True)


    # 2. Ahora que está ordenado, eliminamos los duplicados manteniendo el primero (keep='first')
    df_planes_sin_filtrar = (
        df_planes_sin_filtrar.loc[
            df_planes_sin_filtrar.groupby('id_familia_plan')['fecha'].idxmin()
        ].reset_index(drop=True)
    )

    return df_planes_sin_filtrar

def filtrar_planes_cuidado_verificar_firma(
    df_actividades_consolidados,
    df_planes_firmados,
    filtrar_por_tipo='PLAN DE CUIDADO CREADO',

):
    df_planes = df_actividades_consolidados[
        df_actividades_consolidados['conteo_plan_cuidado'] == filtrar_por_tipo
    ].copy()

    df_planes = df_planes.sort_values(
        by=['id_familia_plan', 'fecha', 'responsable_id'],
        ascending=[True, True, True]
    )

    # If we are filtering CREATED plans and a dataframe of SIGNED plans is provided,
    # keep only created rows that have a matching signed plan with the same responsable_id.

    df_signed_keys = df_planes_firmados[['id_familia_plan', 'responsable_id']].drop_duplicates()
    df_planes = df_planes.merge(df_signed_keys, on=['id_familia_plan', 'responsable_id'], how='inner')

    # drop duplicates keeping the first occurrence
    df_planes = df_planes.drop_duplicates(subset=['fecha', 'id_familia_plan', 'responsable_id'], keep='first').reset_index(drop=True)
    df_planes = df_planes.drop_duplicates(subset=['id_familia_plan', 'responsable_id','conteo_plan_cuidado'], keep='first').reset_index(drop=True)

    return df_planes

def extrer_variable_familia(row, df_familias, variable):

    df_familias['familia_id'] =  df_familias['familia_id'].fillna('nan').astype(str).str.strip().str.replace(r'\.0+$', '', regex=True)
    familia = str(row.get('id_familia_plan')).strip()


    dir_plancuidado = df_familias[
        df_familias['familia_id'] == familia]



    if not dir_plancuidado.empty:
        return dir_plancuidado[variable].iloc[0]
    return '0'


def filtro_actividades(cursor,df_familia, db, df_personas,reporte,client,sheet_id):

    acumulado_actividades = []

    for database in db:
        intervenciones_query = ejecutar_consulta_mysql(query_intervenciones(database), cursor)
        if not intervenciones_query:
            print(f"No se encontraron personas para {database}. Continuando.")
            continue
        acumulado_actividades.extend(intervenciones_query)

    actividades = acumulado_actividades

    df_actividades_consolidados = pd.DataFrame(actividades)

    df_actividades_consolidados.columns = [desc[0] for desc in cursor.description]

    for i, col in enumerate(df_actividades_consolidados.columns):
        if df_actividades_consolidados.dtypes.iloc[i] == object and col not in (
                'historial','fecha'):
            df_actividades_consolidados.iloc[:, i] = df_actividades_consolidados.iloc[:, i].astype(
                str).str.strip().str.replace(r'[^\w\s]', '', regex=True)


    cols_to_clean = ['observacion_id','familia_id','sociambiental_id','juventudadultos_id']
    for col in cols_to_clean:
        df_actividades_consolidados = limpiar_id(df_actividades_consolidados, col)

    df_actividades_consolidados['fecha'] = pd.to_datetime(df_actividades_consolidados['fecha'],
                                                       errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    # df_actividades_consolidados = df_actividades_consolidados.merge(responsables_ebs[['id', 'ebs']], left_on='responsable_id', right_on='id', how='left')


    df_familia['fecha'] = pd.to_datetime(df_familia['fecha'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
        str)

    df_actividades_consolidados['conteo_nuevas_caracterizaciones'] = df_actividades_consolidados.apply(
        lambda row: verificar_nuevas_caracterizaciones(row, df_familia), axis=1
    )

    df_actividades_consolidados['conteo_actualizaciones_ficha'] = df_actividades_consolidados.apply(
        lambda row: verificar_actualizacion_ficha(row, df_familia,df_personas), axis=1
    )
#
    df_actividades_consolidados['conteo_plan_cuidado'] = df_actividades_consolidados.apply(
        lambda row: verificar_plan_cuidado(row, df_familia), axis=1
    )


    df_actividades_consolidados['id_familia_plan'] = df_actividades_consolidados.apply(
        lambda row: row['conteo_plan_cuidado'].split('|')[-1].strip() if row['conteo_plan_cuidado'] != '0' else '0', axis=1
    )
    familia_A_Comparar = df_actividades_consolidados[df_actividades_consolidados['id_familia_plan'] == '79769']

    df_actividades_consolidados['conteo_plan_cuidado'] = df_actividades_consolidados['conteo_plan_cuidado'].apply(lambda x: x.split('|')[0].strip() if x != '0' else '0')

    familia_A_Comparar = df_actividades_consolidados[df_actividades_consolidados['id_familia_plan'] == '79769']


    colums = ['responsable_id', 'fecha','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion', 'conteo_nuevas_caracterizaciones', 'conteo_plan_cuidado','id_familia_plan','conteo_actualizaciones_ficha']

    # colums = ['responsable_id', 'fecha','observacion_id','familia_id','sociambiental_id','juventudadultos_id','responsable_nombre','responsable_profesion','conteo_plan_cuidado','id_familia_plan']

    df_actividades_consolidados = (
        df_actividades_consolidados
        .groupby(colums)
        .size()
        .reset_index(name='count')
        .sort_values(['responsable_id', 'fecha'], ascending=[True, True]))


    df_actividades_consolidados = df_actividades_consolidados.sort_values(['fecha', 'responsable_id'], ascending=[True, True])


    # df_actividades_consolidados = df_actividades_consolidados.drop_duplicates(
    #     subset=['responsable_id', 'observacion_id', 'conteo_plan_cuidado'],
    #     keep='first'
    # ).reset_index(drop=True)

    #df_actividades_consolidados = df_actividades_consolidados[df_actividades_consolidados['responsable_id'].isin(['85','74','1465','1580','182','1417'])]
    df_planes_creados = filtrar_planes_cuidado_creado(df_actividades_consolidados, filtrar_por_tipo='PLAN DE CUIDADO CREADO')

    df_planes_firmados = filtrar_planes_cuidado_verificar_firma(df_actividades_consolidados,df_planes_creados, filtrar_por_tipo='PLAN DE CUIDADO FIRMADO')

    df_planes_creados['dir_anexo'] = df_planes_creados.apply(
        lambda row: extrer_variable_familia(row,df_familia,variable='dirfamiliograma'), axis=1
    )

    df_planes_creados['anexo'] = df_planes_creados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='familiograma'), axis=1
    )

    df_planes_firmados['dir_anexo'] = df_planes_firmados.apply(
        lambda row: extrer_variable_familia(row,df_familia,variable='dirplancuidado'), axis=1
    )

    df_planes_firmados['anexo'] = df_planes_firmados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='plancuidado'), axis=1
    )


    #df_planes_firmados.to_csv('reportes.csv', index=False)


    # no poner aqui los que no seanplan de cuidad ni firmado
    df_actualizaciones_ficha = df_actividades_consolidados[~df_actividades_consolidados['conteo_plan_cuidado'].isin(['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'])]

    df_actividades_consolidados = pd.concat([df_planes_firmados, df_planes_creados, df_actualizaciones_ficha], ignore_index=True)

    df_actividades_consolidados['db_anterior'] = df_actividades_consolidados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='db'), axis=1
    )

    #df_actividades_consolidados['ebs'] = df_actividades_consolidados.merge(responsables_ebs[['id', 'nombre', 'profesion']], left_on='responsable_id', right_on='id', how='left')[['nombre', 'profesion']]

    cargar_actividades(df_actividades_consolidados, reporte, sheet_id, client)



    #df_responsables_plan_cuidado = df_actividades_consolidados[df_actividades_consolidados['conteo_plan_cuidado'] != '0']

    #df_responsables_plan_cuidado['responsables_ebs'] = df_responsables_plan_cuidado.apply(lambda row: agregar_responsables(row, responsables_ebs, df_familia), axis=1)

    #df_responsables_plan_cuidado['responsables_ebs'] = descomponer_responsables(df_responsables_plan_cuidado)['responsable_asignado']

    #cargar_ebs(df_responsables_plan_cuidado, reporte)


def descomponer_responsables(df_responsables_plan_cuidado):
    expanded_rows = []
    for _, r in df_responsables_plan_cuidado.iterrows():
        if r['responsables_ebs'] != '0':
            responsables_str = r['responsables_ebs']
            names = [s.strip() for s in responsables_str.split('|') if s.strip()]
            for name in names:
                new_row = r.copy()
                new_row['responsable_asignado'] = name
                expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows).reset_index(drop=True)


def cargar_ebs (df_responsables_plan_cuidado, reporte):
    df_responsables_plan_cuidado.to_csv(F'../reportes/{reporte}/looker/responsables_plan_cuidado_{reporte}.csv',
                                       index=False)

    cargar_csv_a_bigquery(df_responsables_plan_cuidado, table_id="datos_aps.ebs_planes_cuidado",
                          project_id="aps-project-478903",
                          columnas_fecha=['fecha'])


def cargar_actividades(df_actividades_consolidados, reporte, sheet_id, client):
    df_actividades_consolidados =limpiar_formatos(df_actividades_consolidados, columnas_fecha=['fecha'])
    cargar_csv_a_bigquery(df_actividades_consolidados, table_id="datos_aps.actividades",
                          project_id="aps-project-478903")

    df_actividades_consolidados.to_csv(F'../reportes/{reporte}/looker/consolidado_actividades_{reporte}.csv',
                                       index=False)
    df_actividades_consolidados = df_actividades_consolidados.drop(['observacion_id', 'familia_id','sociambiental_id','juventudadultos_id'], axis=1)
    sobrescribir_hoja(sheet_id, "ACTIVIDADES", df_actividades_consolidados, client)

def verificar_nuevas_caracterizaciones(row, df_familias):
    registro_json = json_to_dict(row)

    sociambiental_id = row.get('sociambiental_id')
    if ['sociambiental_id'] is not None and sociambiental_id != 'nan':
        df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

        if not registro_json.get('updateDate') and  row.get('fecha') > '2025-12-31' and not df_familia.empty:
            if 'validacion' in df_familia.columns:
                no_error_mask = ~df_familia['validacion'].astype(str).str.contains('ERROR EN CARACTERIZACION', na=False)
                count_ok = int(no_error_mask.sum())
                if count_ok > 0:
                    return f" {count_ok} | OK"
                return f" {len(df_familia)} | {df_familia['validacion'].iloc[0]} "
    return '0'


def verificar_actualizacion_ficha(row, df_familias, df_personas):

    try:
        registro_json = json_to_dict(row)

        sociambiental_id = row.get('sociambiental_id') if row.get('sociambiental_id') is not None else 'nan'
        familia_id = row.get('familia_id') if row.get('familia_id') is not None else 'nan'
        juventudadultos_id = row.get('juventudadultos_id') if row.get('juventudadultos_id') is not None else 'nan'
        fecha_de_modificacion = registro_json.get('fecha') if registro_json.get('fecha') else '1900-01-01'

        fecha_de_fila = row.get('fecha')  if row.get('fecha') else '1900-01-01'
        fehca_Actualizacion_fila = registro_json.get('updateDate') if registro_json.get('updateDate') else False

        observacion_id = row.get('observacion_id') if row.get('observacion_id') is not None else 'nan'
        dir_familiograma = registro_json.get('dirfamilliograma') if registro_json.get('dirfamilliograma') else None

        if ['sociambiental_id'] is not None and sociambiental_id != 'nan':


            df_familia = df_familias[df_familias['sociambiental_id'] == int(float(sociambiental_id))]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE |C"

            if fehca_Actualizacion_fila and fecha_de_fila > fecha_de_modificacion:
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE FICHA VIVIENDA |C"

                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |C"

        if ['familia_id'] is not None and familia_id != 'nan':
                df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

                if df_familia.empty:
                    return " 1 | VERIFICAR FAMILIA NO TIENE |C"

                if row.get('fecha') > df_familia['fecha'].iloc[0]:
                    if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                        return f" 1 | ACTUALIZACION DE FICHA FAMILIA |C"
                    return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |C"


        if['juventudadultos_id'] is not None and juventudadultos_id != 'nan':
            df_personas['id'] = df_personas['id'].fillna('nan').astype(str).str.strip().str.replace(r'\.0+$', '', regex=True)
            df_persona = df_personas[df_personas['juventud_id'] == int(float(juventudadultos_id))]

            if df_persona.empty:
                return " 1 | VERIFICAR PERSONA NO EXISTE |C"

            df_familia = df_familias[df_familias['familia_id'] == df_persona['familia_id'].iloc[0]]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE  |C"

            fecha_creacion = df_familia['fecha'].iloc[0]
            fe = fecha_de_modificacion

            if pd.to_datetime(fecha_de_fila, errors='coerce') > (
                    pd.to_datetime(fecha_creacion, errors='coerce') + pd.Timedelta(days=30)):
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE FICHA PERSONA {df_persona['doc_id'].iloc[0]} | O"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} | O"

        if['observacion_id'] is not None and observacion_id != 'nan' and familia_id != 'nan' :

            df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

            if df_familia.empty:
                return " 1 | VERIFICAR FAMILIA NO TIENE  |O"

            if dir_familiograma is not None:
                if not df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                    return f" 1 | ACTUALIZACION DE OBSERVACION {df_familia['id'].iloc[0]} |O"
                return f" 1 | {str(df_familia['validacion'].iloc[0]).strip()} |O"

        return '0'
    except Exception as e:
        print(f"Error en verificar_actualizacion_ficha: {e}")
        return '0'


def json_to_dict(row,column_name='historial'):
    registro_json = row.get(column_name , '')

    if isinstance(registro_json, str):
        try:
            registro_json = json.loads(registro_json)
        except Exception:
            try:
                registro_json = ast.literal_eval(registro_json)
            except Exception:
                registro_json = {}
    else:
        registro_json = registro_json or {}

    return registro_json

def verificar_plan_cuidado(row, df_familias):
    try:
        registro_json = json_to_dict(row)

        observacion_id = row.get('observacion_id')

        plan_de_cuidado = registro_json.get('plancuidado')

        if ['observacion_id'] is not None and observacion_id != 'nan' and not registro_json.get('dirfamilliograma'):
            familia_id = registro_json.get('familia_id')
            df_familia = df_familias[df_familias['familia_id'] == int(float(familia_id))]

            if df_familia.empty:
                return f"PLAN DE CUIDADO NO VALIDO  | {familia_id}"

            if df_familia['validacion'].str.contains('ERROR EN CARACTERIZACION').any():
                return f"PLAN DE CUIDADO CON {str(df_familia['validacion'].iloc[0]).strip()}   | {familia_id}"
            if plan_de_cuidado :
                return f"PLAN DE CUIDADO FIRMADO | {familia_id}"
            if registro_json.get('actividaddesarrollar'):
                return f"PLAN DE CUIDADO CREADO | {familia_id}"
            if registro_json.get('actividaddesarrollar') == '':
                    return f"PLAN DE CUIDADO NO VALIDO | {familia_id}"
            else :
                return f"0"

        return '0'
    except Exception as e:
        print(f"Error en verificar_plan_cuidado: {e}")
        return f"PLAN DE CUIDADO NO VALIDO ERROR"


def agregar_responsables(row, df_responsables,df_familias):

    if row.get('conteo_plan_cuidado') == 'PLAN DE CUIDADO FIRMADO':
        id_familia_plan = row.get('id_familia_plan')
        df_familia = df_familias[df_familias['familia_id'] == int(float(id_familia_plan))]

        if not df_familia.empty:
            ebs = df_familia['involucrado_plan_cuidado']

            if pd.isna(ebs) or ebs == 'nan' or ebs.strip() == '':
                return "0"

            array_validacion = ebs.split('|') if pd.notna(ebs) else []

            responsables = ''

            for i in range(len(array_validacion)):
                array_validacion[i] = array_validacion[i].strip()
                responsable = df_responsables[df_responsables['id'].isin(i)]
                if not responsable.empty:
                    responsables += responsable['nombre'].iloc[0] + ' | '

            return responsables
    return "0"









