import pandas as pd

from automatizacion_looker_aps.query.query_persona import query_persona
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.limpiar_canalizaciones import limpiar_y_extraer
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from mysql_conector import ejecutar_consulta_mysql


# ───────────────────────── 1. REGLAS DE DATOS (config) ─────────────────────────
CORREO_ASEGURADORAS = "pastosaludeseaps@gmail.com"
ASEGURADORAS_CON_CORREO = {
    "EMSSANAR", "MALLAMAS", "SANITAS", "PROINSALUD", "FOMAC",
    "FAMISANAR", "FAMISSANAR", "NUEVA EPS",
}

COLS_ID = ["familia_id", "sociambiental_id", "numero"]
COLS_FECHA = ["fecha", "fechanac"]
COLS_SIN_LIMPIAR_TEXTO = ("canalizacionuno",)
COLUMNAS_REQUERIDAS = [
    "familia_id", "sociambiental_id", "numero", "doc_id",
    "fecha", "fechanac", "territorio", "aseguradora", "canalizacionuno",
]

# ───────────────────────── 2. EXTRACT ─────────────────────────
def extraer_personas(cursor, bases: list[str]) -> pd.DataFrame:
    frames = []
    for base in bases:
        filas = ejecutar_consulta_mysql(query_persona(base), cursor)
        if not filas:
            print(f"[extract] Sin registros en {base}")
            continue
        columnas = [d[0] for d in cursor.description]
        df = pd.DataFrame(filas, columns=columnas)
        df["base_origen"] = base          # trazabilidad: de dónde vino cada fila
        frames.append(df)

    if not frames:
        raise ValueError("Extract: ninguna base devolvió personas")

    df = pd.concat(frames, ignore_index=True)
    print(f"[extract] {len(df)} filas de {len(frames)} bases")
    return df

# ───────────────────────── 3. TRANSFORM (funciones pequeñas) ─────────────────────────
def _limpiar_id(s: pd.Series) -> pd.Series:
    return (s.fillna("").astype(str).str.strip()
             .str.replace(r"\.0+$", "", regex=True))

def _calcular_edad(nac: pd.Series, hoy: pd.Timestamp) -> pd.Series:
    edad = (hoy.year - nac.dt.year
            - ((nac.dt.month > hoy.month) |
               ((nac.dt.month == hoy.month) & (nac.dt.day > hoy.day))).astype(int))
    return edad.astype("Int64").astype(str).replace("<NA>", "")

def limpiar_ids(df):
    df = df.copy()
    for c in COLS_ID:
        df[c] = _limpiar_id(df[c])
    df["id"] = df["familia_id"]
    df["doc_id"] = df["doc_id"].astype(str).str.replace(r"\D+", "", regex=True)
    return df

def limpiar_texto(df):
    df = df.copy()
    cols_texto = [c for c in df.select_dtypes(include="object").columns
                  if c not in COLS_SIN_LIMPIAR_TEXTO]
    for c in cols_texto:
        df[c] = (df[c].fillna("").astype(str).str.strip()
                   .str.replace(r"[^\w\s]", "", regex=True))
    return df

def procesar_fechas(df):
    df = df.copy()
    hoy = pd.Timestamp.today().normalize()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["fechanac"] = pd.to_datetime(df["fechanac"], errors="coerce")
    df["edad"] = _calcular_edad(df["fechanac"], hoy)
    for c in COLS_FECHA:
        df[c] = df[c].dt.strftime("%Y-%m-%d").fillna("")
    return df

def enriquecer_redes(df, df_redes):
    df = df.copy()
    mapa = df_redes.drop_duplicates("TERRITORIO").set_index("TERRITORIO")["RED"]
    df["redes"] = df["territorio"].map(mapa).fillna("")
    return df

def separar_canalizaciones(df, i=None):
    df = df.copy()
    df["canalizacionuno"] = df["canalizacionuno"].str.replace(",", "$", regex=False)
    for i in range(3):
        df[f"canalizacion_{i+1}"] = df["canalizacionuno"].apply(
            lambda x, i=i: limpiar_y_extraer(x, i))
    return df

def asignar_correo_aseguradora(df):
    df = df.copy()
    es_valida = df["aseguradora"].str.upper().str.strip().isin(ASEGURADORAS_CON_CORREO)
    df["correo_aseguradora_correspondiente"] = es_valida.map(
        {True: CORREO_ASEGURADORAS, False: ""})
    return df

def transformar_personas(df, df_redes, fe_reporte):
    df = (df
          .pipe(limpiar_ids)
          .pipe(limpiar_texto)
          .pipe(procesar_fechas)
          .pipe(enriquecer_redes, df_redes)
          .pipe(separar_canalizaciones)
          .pipe(asignar_correo_aseguradora))
    df["reporte_fecha"] = fe_reporte
    return df

# ───────────────────────── 4. VALIDAR (antes de cargar) ─────────────────────────
def validar_personas(df: pd.DataFrame) -> pd.DataFrame:
    faltantes = set(COLUMNAS_REQUERIDAS) - set(df.columns)
    assert not faltantes, f"Faltan columnas: {faltantes}"
    assert len(df) > 0, "DataFrame vacío"
    assert (df["familia_id"] != "").all(), "Hay filas sin familia_id"

    sin_red = (df["redes"] == "").sum()
    if sin_red:
        print(f"[calidad] ⚠ {sin_red} personas sin red asignada")
    dup = df.duplicated(subset=["familia_id", "doc_id"]).sum()
    if dup:
        print(f"[calidad] ⚠ {dup} duplicados familia_id+doc_id")
    return df

# ───────────────────────── 5. LOAD (cada destino por separado) ─────────────────────────
def cargar_csv(df, fe_reporte):
    ruta = f"../reportes/{fe_reporte}/looker/cosolidado_personas_{fe_reporte}.csv"
    df.to_csv(ruta, index=False)

def cargar_bigquery(df):
    df = limpiar_formatos(df, columnas_fecha=["fechanac", "fecha", "reporte_fecha"])
    cargar_csv_a_bigquery(df, table_id="datos_aps.personas",
                          project_id="aps-project-478903")
    return df

def cargar_sheets(df, sheet_id, client):
    sobrescribir_hoja(sheet_id, "cosolidado_personas", df, client)

# ───────────────────────── 6. ORQUESTACIÓN ─────────────────────────
def etl_personas(cursor, df_redes, fe_reporte, client, bases, sheet_id):
    crudo   = extraer_personas(cursor, bases)
    limpio  = transformar_personas(crudo, df_redes, fe_reporte)
    final   = validar_personas(limpio)

    if fe_reporte:
        cargar_csv(final, fe_reporte)
        final = cargar_bigquery(final)
        cargar_sheets(final, sheet_id, client)
    return final