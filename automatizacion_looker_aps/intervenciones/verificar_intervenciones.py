import json
import logging
import pandas as pd

from automatizacion_looker_aps.intervenciones.filtrado_actividades import json_to_dict
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja

# Configuración básica de logs para trazabilidad
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==========================================
# UTILIDADES DE LIMPIEZA / AUXILIARES
# ==========================================
def forzar_diccionario(dato):
    """Convierte de forma segura cadenas JSON o estructuras anidadas a un diccionario."""
    if pd.isna(dato):
        return {}

    if isinstance(dato, list) and len(dato) > 0:
        dato = dato[0]

    if isinstance(dato, dict):
        return dato

    try:
        resultado = json.loads(str(dato))
        if isinstance(resultado, list) and len(resultado) > 0:
            resultado = resultado[0]

        if isinstance(resultado, str):
            resultado = json.loads(resultado)
            if isinstance(resultado, list) and len(resultado) > 0:
                resultado = resultado[0]

        return resultado if isinstance(resultado, dict) else {}

    except (json.JSONDecodeError, TypeError):
        return {}


# ==========================================
# 1. PASO: EXTRACT (Extracción)
# ==========================================
def extraer_datos_validos(df_intervenciones):
    """
    Fase Extract: Filtra los registros que cuentan con información
    en la columna clave 'actividaddesarrollar'.
    """
    logging.info("Iniciando fase de Extracción (Extract)...")
    if 'actividaddesarrollar' not in df_intervenciones.columns:
        raise KeyError("La columna 'actividaddesarrollar' no está presente en el DataFrame de entrada.")

    df_validas = df_intervenciones.dropna(subset=['actividaddesarrollar']).copy()
    logging.info(f"Extracción finalizada. Registros a procesar: {len(df_validas)}")
    return df_validas


# ==========================================
# 2. PASO: TRANSFORM (Transformación)
# ==========================================
def desanidar_situaciones_json(df_validas):
    """Extrae las situaciones priorizadas desde la columna JSON 'actividaddesarrollar'."""
    filas_procesadas = []

    for _, row in df_validas.iterrows():
        registro_json = json_to_dict(row, 'actividaddesarrollar')

        # Normalizar el contenido a lista de diccionarios
        if isinstance(registro_json, dict):
            registro_json = [registro_json]
        elif not isinstance(registro_json, list):
            continue

        for situacion in registro_json:
            if not isinstance(situacion, dict):
                continue

            filas_procesadas.append({
                'id_familia_plan': row.get('familia_id', ''),
                'id_plan_cuidado': row.get('observacion_id', ''),
                'id_sociambiental': row.get('sociambiental_id', ''),
                'situacion_priorizada': situacion.get('situacionesPriorizadas', ''),
                'estado': situacion.get('estado', ''),
                'fecha_compromiso': situacion.get('fechaCompromiso', ''),
                'fecha_seguimiento': situacion.get('fechaSeguimiento', ''),
                'objetivo': str(situacion.get('objetivoCortoPlazo', '')),
                'resultado': str(situacion.get('resultadosEsperados', '')),
                'id_persona': situacion.get('responsableFamilia', ''),
                'id_responsable': situacion.get('responsableEBS', '')
            })

    return pd.DataFrame(filas_procesadas)


def transformar_situaciones_priorizadas(df_validas, df_familia, df_responsables):
    """
    Fase Transform: Desanida JSONs, realiza cruces de información (merges),
    limpia caracteres especiales y formatea los tipos de datos.
    """
    logging.info("Iniciando fase de Transformación (Transform)...")

    # A. Desanidar JSONs a un DataFrame plano
    df_transformado = desanidar_situaciones_json(df_validas)

    if df_transformado.empty:
        logging.warning("No se generaron registros válidos tras desanidar los datos JSON.")
        return df_transformado

    # B. Merge con Datos de Familia (Territorio y Microterritorio)
    if not df_familia.empty and 'sociambiental_id' in df_familia.columns:
        df_transformado = df_transformado.merge(
            df_familia[['sociambiental_id', 'territorio', 'microterritorio']],
            left_on='id_sociambiental',
            right_on='sociambiental_id',
            how='left'
        ).drop(columns=['sociambiental_id'], errors='ignore')

    # C. Limpieza de textos en columnas de descripción
    for col in ['resultado', 'objetivo']:
        if col in df_transformado.columns:
            df_transformado[col] = (
                df_transformado[col]
                .astype(str)
                .str.replace(',', '$', regex=False)
                .str.replace(r'[\[\]"]', '', regex=True)
                .str.strip()
            )

    # D. Estandarización de llaves numéricas e integración con Responsables
    df_transformado['id_responsable'] = pd.to_numeric(
        df_transformado['id_responsable'], errors='coerce'
    ).astype('Int64')

    if not df_responsables.empty and 'id' in df_responsables.columns:
        df_resp = df_responsables.copy()
        df_resp['id'] = pd.to_numeric(df_resp['id'], errors='coerce').astype('Int64')

        df_transformado = df_transformado.merge(
            df_resp[['id', 'nombres']],
            left_on='id_responsable',
            right_on='id',
            how='left'
        )

    logging.info(f"Transformación finalizada. Filas resultantes: {len(df_transformado)}")
    return df_transformado


# ==========================================
# 3. PASO: LOAD (Carga)
# ==========================================
def cargar_resultados(df_final, sheet_id, client, project_id="aps-project-478903", table_id="datos_aps.situaciones_priorizadas"):
    """
    Fase Load: Publica los datos limpios en los destinos finales
    (BigQuery y Google Sheets).
    """
    logging.info("Iniciando fase de Carga (Load)...")

    # 1. Carga a Google BigQuery
    cargar_csv_a_bigquery(
        df_final,
        table_id=table_id,
        project_id=project_id
    )
    logging.info(f"Datos cargados exitosamente en BigQuery [{table_id}].")

    # 2. Carga/Sobrescritura en Google Sheets
    sobrescribir_hoja(sheet_id, "ST", df_final, client)
    logging.info(f"Hoja 'ST' actualizada correctamente en Google Sheets [{sheet_id}].")


# ==========================================
# ORQUESTADOR PRINCIPAL (ETL Pipeline)
# ==========================================
def etl_verificar_indicadores(df_intervenciones, sheet_id, client, df_responsables, df_familia):
    """Orquesta las tres fases principales de la tubería ETL."""
    try:
        logging.info("=== INICIANDO PIPELINE ETL: INDICADORES / SITUACIONES PRIORIZADAS ===")

        # 1. EXTRACT
        df_validas = extraer_datos_validos(df_intervenciones)

        # 2. TRANSFORM
        df_final = transformar_situaciones_priorizadas(df_validas, df_familia, df_responsables)

        if df_final.empty:
            logging.warning("Proceso detenido: No hay datos procesados para cargar.")
            return

        # 3. LOAD
        cargar_resultados(df_final, sheet_id, client)

        logging.info("=== PIPELINE ETL COMPLETADO EXITOSAMENTE ===")

    except Exception as e:
        logging.error(f"Error crítico durante la ejecución del ETL: {e}", exc_info=True)
        raise