from contextlib import closing
from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from automatizacion_looker_aps.personas.main import (
    conectar_sheets, conectar_mysql, preparar_entorno, SHEET_ID_PERSONAS,
)
from automatizacion_looker_aps.personas.personas import (
    extraer_personas, transformar_personas, validar_personas,
    cargar_csv, cargar_bigquery, cargar_sheets,
)
from credenciales import DATABASE
from limpieza_datos import extraer_distribucion_redes

G_PREP = "1_preparacion"
G_EXTRACT = "2_extract"
G_TRANSFORM = "3_transform"
G_LOAD = "4_load"
REINTENTOS = RetryPolicy(max_retries=2, delay=30)


# ───────────── RECURSO: el client de gspread no se puede guardar entre pasos,
# ───────────── así que cada asset que lo necesita lo pide aquí.
class GoogleSheetsResource(ConfigurableResource):
    def get_client(self):
        return conectar_sheets()


def _meta(context: AssetExecutionContext, df: pd.DataFrame):
    """Muestra filas y columnas en la UI de cada asset."""
    context.add_output_metadata({"filas": len(df), "columnas": len(df.columns)})


# ───────────── PREPARACIÓN ─────────────
@asset(group_name=G_PREP, description="Fecha del reporte (YYYY-MM-DD)")
def fe_reporte(context: AssetExecutionContext) -> str:
    fe = datetime.now().strftime("%Y-%m-%d")
    context.log.info(f"Inicio ETL personas | reporte {fe}")
    return fe


@asset(group_name=G_PREP, retry_policy=REINTENTOS,
       description="Verifica credenciales y conexión con Google")
def sheets_conectado(context: AssetExecutionContext, sheets: GoogleSheetsResource) -> None:
    sheets.get_client()
    context.log.info("Conexión a Google verificada")


@asset(group_name=G_PREP, description="Crea la carpeta del reporte")
def carpetas_reporte(fe_reporte: str) -> None:
    preparar_entorno(fe_reporte)


@asset(group_name=G_EXTRACT, description="Extract: tabla de distribución de redes")
def distribucion_redes(context: AssetExecutionContext) -> pd.DataFrame:
    df = extraer_distribucion_redes()
    _meta(context, df)
    return df


# ───────────── ETL ─────────────
@asset(group_name=G_EXTRACT, deps=[sheets_conectado, carpetas_reporte],
       description="Extract: personas desde MySQL (una query por base)")
def personas_crudo(context: AssetExecutionContext) -> pd.DataFrame:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        df = extraer_personas(cur, DATABASE)
    _meta(context, df)
    return df


@asset(group_name=G_TRANSFORM,
       description="Transform: limpieza, fechas, redes, canalizaciones, correos")
def personas_transformadas(
    context: AssetExecutionContext,
    personas_crudo: pd.DataFrame,
    distribucion_redes: pd.DataFrame,
    fe_reporte: str,
) -> pd.DataFrame:
    df = transformar_personas(personas_crudo, distribucion_redes, fe_reporte)
    _meta(context, df)
    return df


@asset(group_name=G_TRANSFORM,
       description="Validar calidad: si falla, no se carga nada")
def personas_validadas(
    context: AssetExecutionContext,
    personas_transformadas: pd.DataFrame,
) -> pd.DataFrame:
    df = validar_personas(personas_transformadas)
    _meta(context, df)
    return df


# ───────────── LOAD ─────────────
@asset(group_name=G_LOAD, description="Load: CSV local para Looker")
def personas_csv(personas_validadas: pd.DataFrame, fe_reporte: str) -> None:
    cargar_csv(personas_validadas, fe_reporte)


@asset(group_name=G_LOAD, retry_policy=REINTENTOS,
       description="Load: tabla datos_aps.personas en BigQuery")
def personas_bigquery(
    context: AssetExecutionContext,
    personas_validadas: pd.DataFrame,
) -> pd.DataFrame:
    df = cargar_bigquery(personas_validadas)
    _meta(context, df)
    return df


@asset(group_name=G_LOAD, retry_policy=REINTENTOS,
       description="Load: hoja cosolidado_personas en Google Sheets")
def personas_sheets(
    context: AssetExecutionContext,
    personas_bigquery: pd.DataFrame,
    sheets: GoogleSheetsResource,
) -> None:
    cargar_sheets(personas_bigquery, SHEET_ID_PERSONAS, sheets.get_client())
    context.log.info(f"ETL terminado: {len(personas_bigquery)} personas")


# ───────────── JOB, SCHEDULE Y DEFINICIONES ─────────────
job_personas = define_asset_job(
    name="etl_personas_job",
    selection=[
        fe_reporte, sheets_conectado, carpetas_reporte, distribucion_redes,
        personas_crudo, personas_transformadas, personas_validadas,
        personas_csv, personas_bigquery, personas_sheets,
    ],
)

schedule_diario = ScheduleDefinition(
    job=job_personas,
    cron_schedule="0 7 * * *",       # todos los días a las 7:00
    execution_timezone="America/Bogota",
)

defs = Definitions(
    assets=[
        fe_reporte, sheets_conectado, carpetas_reporte, distribucion_redes,
        personas_crudo, personas_transformadas, personas_validadas,
        personas_csv, personas_bigquery, personas_sheets,
    ],
    jobs=[job_personas],
    schedules=[schedule_diario],
    resources={"sheets": GoogleSheetsResource()},
)