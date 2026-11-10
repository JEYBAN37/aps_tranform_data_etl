from contextlib import closing
from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    ConfigurableResource,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)

# Si moviste main.py, ajusta esta ruta
from automatizacion_looker_aps.personas.main import (
    conectar_sheets, conectar_mysql, preparar_entorno,
    SHEET_ID_PERSONAS, SHEET_ID_FAMILIAS, SHEET_ID_NOVEDADES,
    SHEET_ID_ACTIVIDADES, SHEET_ID_INDICADORES,
)
from automatizacion_looker_aps.personas.personas import (
    extraer_personas, transformar_personas, validar_personas,
    cargar_csv, cargar_bigquery, cargar_sheets,
)
from automatizacion_looker_aps.familias.familias import (
    extraer_familias, transformar_familias, validar_familias,
    formatear_para_carga, extraer_situaciones_prioritarias, preparar_familias_final,
    cargar_csv_familias, cargar_bigquery_familias, cargar_sheets_familias,
)
from automatizacion_looker_aps.intervenciones.filtrado_actividades import (
    extraer_actividades, transformar_actividades, validar_actividades,
    formatear_actividades, cargar_csv_actividades, cargar_bigquery_actividades,
    cargar_sheets_actividades,
)
from automatizacion_looker_aps.intervenciones.verificar_intervenciones import etl_verificar_indicadores
from automatizacion_looker_aps.novedades.novedades import etl_novedades
from automatizacion_looker_aps.responsables.responsable import extraer_responsables
from credenciales import DATABASE
from limpieza_datos import extraer_distribucion_redes

# Cada grupo es un cuadro en el grafo de Dagster (uno por flujo)
G_PREP = "preparacion"
G_PERSONAS = "personas"
G_FAMILIAS = "familias"
G_RESPONSABLES = "responsables"
G_NOVEDADES = "novedades"
G_INDICADORES = "indicadores"
G_ACTIVIDADES = "actividades"

REINTENTOS = RetryPolicy(max_retries=2, delay=30)

# Máximo 2 pasos a la vez: evita saturar la réplica de MySQL y la cuota de la API de Sheets
LIMITE_CONCURRENCIA = {"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}}


# ───────────── RECURSO: el client de gspread no se puede guardar entre pasos
class GoogleSheetsResource(ConfigurableResource):
    def get_client(self):
        return conectar_sheets()


def _meta(context: AssetExecutionContext, df: pd.DataFrame):
    context.add_output_metadata({"filas": len(df), "columnas": len(df.columns)})


# ═════════════════════ PREPARACIÓN (compartida) ═════════════════════
@asset(group_name=G_PREP, description="Fecha del reporte (YYYY-MM-DD)")
def fe_reporte(context: AssetExecutionContext) -> str:
    fe = datetime.now().strftime("%Y-%m-%d")
    context.log.info(f"Inicio ETL | reporte {fe}")
    return fe


@asset(group_name=G_PREP, retry_policy=REINTENTOS,
       description="Verifica credenciales y conexión con Google")
def sheets_conectado(context: AssetExecutionContext, sheets: GoogleSheetsResource) -> None:
    sheets.get_client()
    context.log.info("Conexión a Google verificada")


@asset(group_name=G_PREP, description="Crea la carpeta del reporte")
def carpetas_reporte(fe_reporte: str) -> None:
    preparar_entorno(fe_reporte)


@asset(group_name=G_PREP, description="Tabla de distribución de redes (la usan varios ETL)")
def distribucion_redes(context: AssetExecutionContext) -> pd.DataFrame:
    df = extraer_distribucion_redes()
    _meta(context, df)
    return df


# ═════════════════════ PERSONAS ═════════════════════
@asset(group_name=G_PERSONAS, deps=[sheets_conectado, carpetas_reporte],
       description="Extract: personas desde MySQL (una query por base)")
def personas_crudo(context: AssetExecutionContext) -> pd.DataFrame:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        df = extraer_personas(cur, DATABASE)
    _meta(context, df)
    return df


@asset(group_name=G_PERSONAS,
       description="Transform: limpieza, fechas, redes, canalizaciones, correos")
def personas_transformadas(context: AssetExecutionContext, personas_crudo: pd.DataFrame,
                           distribucion_redes: pd.DataFrame, fe_reporte: str) -> pd.DataFrame:
    df = transformar_personas(personas_crudo, distribucion_redes, fe_reporte)
    _meta(context, df)
    return df


@asset(group_name=G_PERSONAS, description="Validar calidad: si falla, no se carga nada")
def personas_validadas(context: AssetExecutionContext,
                       personas_transformadas: pd.DataFrame) -> pd.DataFrame:
    df = validar_personas(personas_transformadas)
    _meta(context, df)
    return df


@asset(group_name=G_PERSONAS, description="Load: CSV local para Looker")
def personas_csv(personas_validadas: pd.DataFrame, fe_reporte: str) -> None:
    cargar_csv(personas_validadas, fe_reporte)


@asset(group_name=G_PERSONAS, retry_policy=REINTENTOS,
       description="Load: tabla datos_aps.personas en BigQuery")
def personas_bigquery(context: AssetExecutionContext,
                      personas_validadas: pd.DataFrame) -> pd.DataFrame:
    df = cargar_bigquery(personas_validadas)
    _meta(context, df)
    return df


@asset(group_name=G_PERSONAS, retry_policy=REINTENTOS,
       description="Load: hoja cosolidado_personas en Google Sheets")
def personas_sheets(context: AssetExecutionContext, personas_bigquery: pd.DataFrame,
                    sheets: GoogleSheetsResource) -> None:
    cargar_sheets(personas_bigquery, SHEET_ID_PERSONAS, sheets.get_client())
    context.log.info(f"Personas cargadas: {len(personas_bigquery)}")


# ═════════════════════ FAMILIAS ═════════════════════
@asset(group_name=G_FAMILIAS, deps=[sheets_conectado, carpetas_reporte],
       description="Extract: familias desde MySQL (una query por base)")
def familias_crudo(context: AssetExecutionContext) -> pd.DataFrame:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        df = extraer_familias(cur, DATABASE)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS,
       description="Transform: duplicados, coordenadas, listas, texto, fechas, validación, redes")
def familias_transformadas(context: AssetExecutionContext, familias_crudo: pd.DataFrame,
                           distribucion_redes: pd.DataFrame, fe_reporte: str) -> pd.DataFrame:
    df = transformar_familias(familias_crudo, distribucion_redes, fe_reporte)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS, description="Validar calidad: si falla, no se carga nada")
def familias_validadas(context: AssetExecutionContext,
                       familias_transformadas: pd.DataFrame) -> pd.DataFrame:
    df = validar_familias(familias_transformadas)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS, description="Formatos de fecha listos para BigQuery y Sheets")
def familias_formateadas(context: AssetExecutionContext,
                         familias_validadas: pd.DataFrame) -> pd.DataFrame:
    df = formatear_para_carga(familias_validadas)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS,
       description="Situaciones prioritarias (actividad a desarrollar, involucrados)")
def situaciones_prioritarias(context: AssetExecutionContext,
                             familias_formateadas: pd.DataFrame) -> pd.DataFrame:
    df = extraer_situaciones_prioritarias(familias_formateadas)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS, description="Familias sin las columnas de situaciones prioritarias")
def familias_final(context: AssetExecutionContext,
                   familias_formateadas: pd.DataFrame) -> pd.DataFrame:
    df = preparar_familias_final(familias_formateadas)
    _meta(context, df)
    return df


@asset(group_name=G_FAMILIAS, description="Load: CSV local para Looker")
def familias_csv(familias_validadas: pd.DataFrame, fe_reporte: str) -> None:
    cargar_csv_familias(familias_validadas, fe_reporte)


@asset(group_name=G_FAMILIAS, retry_policy=REINTENTOS,
       description="Load: tabla datos_aps.familias en BigQuery")
def familias_bigquery(familias_final: pd.DataFrame) -> None:
    cargar_bigquery_familias(familias_final)


@asset(group_name=G_FAMILIAS, retry_policy=REINTENTOS,
       description="Load: hoja cosolidado_familias en Google Sheets")
def familias_sheets(context: AssetExecutionContext, familias_final: pd.DataFrame,
                    sheets: GoogleSheetsResource) -> None:
    cargar_sheets_familias(familias_final, SHEET_ID_FAMILIAS, sheets.get_client())
    context.log.info(f"Familias cargadas: {len(familias_final)}")


# ═════════════════════ RESPONSABLES ═════════════════════
@asset(group_name=G_RESPONSABLES, deps=[sheets_conectado, carpetas_reporte],
       description="Extract: responsables desde MySQL (insumo de indicadores)")
def responsables_crudo(context: AssetExecutionContext) -> pd.DataFrame:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        df = extraer_responsables(cur, DATABASE)
    _meta(context, df)
    return df


# ═════════════════════ NOVEDADES ═════════════════════
# etl_novedades hace extract, transform y load en una sola función, así que aquí
# es un solo asset. Sin reintentos automáticos: no sé si su carga es repetible.
@asset(group_name=G_NOVEDADES, deps=[sheets_conectado, carpetas_reporte],
       description="ETL completo de novedades (extract + transform + load)")
def novedades(distribucion_redes: pd.DataFrame, fe_reporte: str,
              sheets: GoogleSheetsResource) -> None:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        etl_novedades(cursor=cur, df_redes=distribucion_redes, fe_reporte=fe_reporte,
                      client=sheets.get_client(), bases=DATABASE, sheet_id=SHEET_ID_NOVEDADES)


# ═════════════════════ INDICADORES ═════════════════════
@asset(group_name=G_INDICADORES,
       description="Verifica indicadores con situaciones prioritarias, responsables y familias")
def indicadores_verificados(situaciones_prioritarias: pd.DataFrame,
                            responsables_crudo: pd.DataFrame,
                            familias_final: pd.DataFrame,
                            sheets: GoogleSheetsResource) -> None:
    etl_verificar_indicadores(
        df_intervenciones=situaciones_prioritarias,
        sheet_id=SHEET_ID_INDICADORES,
        client=sheets.get_client(),
        df_responsables=responsables_crudo,
        df_familia=familias_final,
    )


# ═════════════════════ ACTIVIDADES ═════════════════════
@asset(group_name=G_ACTIVIDADES, deps=[sheets_conectado, carpetas_reporte],
       description="Extract: intervenciones desde MySQL")
def actividades_crudo(context: AssetExecutionContext) -> pd.DataFrame:
    with closing(conectar_mysql()) as cn, closing(cn.cursor()) as cur:
        df = extraer_actividades(cur, DATABASE)
    _meta(context, df)
    return df


@asset(group_name=G_ACTIVIDADES,
       description="Transform: clasifica cada intervención y selecciona planes creados y firmados")
def actividades_transformadas(context: AssetExecutionContext, actividades_crudo: pd.DataFrame,
                              familias_final: pd.DataFrame,
                              personas_validadas: pd.DataFrame) -> pd.DataFrame:
    df = transformar_actividades(actividades_crudo, familias_final, personas_validadas)
    _meta(context, df)
    return df


@asset(group_name=G_ACTIVIDADES, description="Validar calidad: si falla, no se carga nada")
def actividades_validadas(context: AssetExecutionContext,
                          actividades_transformadas: pd.DataFrame) -> pd.DataFrame:
    df = validar_actividades(actividades_transformadas)
    _meta(context, df)
    return df


@asset(group_name=G_ACTIVIDADES, description="Formatos de fecha listos para cargar")
def actividades_formateadas(context: AssetExecutionContext,
                            actividades_validadas: pd.DataFrame) -> pd.DataFrame:
    df = formatear_actividades(actividades_validadas)
    _meta(context, df)
    return df


@asset(group_name=G_ACTIVIDADES, description="Load: CSV local para Looker")
def actividades_csv(actividades_formateadas: pd.DataFrame, fe_reporte: str) -> None:
    cargar_csv_actividades(actividades_formateadas, fe_reporte)


@asset(group_name=G_ACTIVIDADES, retry_policy=REINTENTOS,
       description="Load: tabla datos_aps.actividades en BigQuery")
def actividades_bigquery(actividades_formateadas: pd.DataFrame) -> None:
    cargar_bigquery_actividades(actividades_formateadas)


@asset(group_name=G_ACTIVIDADES, retry_policy=REINTENTOS,
       description="Load: hoja ACTIVIDADES en Google Sheets")
def actividades_sheets(context: AssetExecutionContext, actividades_formateadas: pd.DataFrame,
                       sheets: GoogleSheetsResource) -> None:
    cargar_sheets_actividades(actividades_formateadas, SHEET_ID_ACTIVIDADES, sheets.get_client())
    context.log.info(f"Actividades cargadas: {len(actividades_formateadas)}")


# ═════════════════════ JOBS, SCHEDULE Y DEFINICIONES ═════════════════════
def _job(nombre: str, *assets_finales: str):
    """Un job que ejecuta esos assets y todo lo que necesitan antes."""
    return define_asset_job(
        nombre,
        selection=AssetSelection.assets(*assets_finales).upstream(),
        config=LIMITE_CONCURRENCIA,
    )


job_completo = define_asset_job("etl_completo_job", selection=AssetSelection.all(),
                                config=LIMITE_CONCURRENCIA)
job_personas = _job("etl_personas_job", "personas_csv", "personas_sheets")
job_familias = _job("etl_familias_job", "familias_csv", "familias_bigquery", "familias_sheets")
job_novedades = _job("etl_novedades_job", "novedades")
job_indicadores = _job("etl_indicadores_job", "indicadores_verificados")
job_actividades = _job("etl_actividades_job", "actividades_csv", "actividades_bigquery",
                       "actividades_sheets")

schedule_diario = ScheduleDefinition(
    job=job_completo,
    cron_schedule="0 7 * * *",          # todos los días a las 7:00
    execution_timezone="America/Bogota",
)

defs = Definitions(
    assets=load_assets_from_current_module(),
    jobs=[job_completo, job_personas, job_familias, job_novedades,
          job_indicadores, job_actividades],
    schedules=[schedule_diario],
    resources={"sheets": GoogleSheetsResource()},
)