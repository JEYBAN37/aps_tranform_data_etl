from automatizacion_looker_aps.query.query_novedades import query_novedades
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery, limpiar_formatos
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja
from export_aps_124 import limpiar_formato_latitud, limpiar_formato_longitud
from mysql_conector import ejecutar_consulta_mysql
import logging
import re
import unicodedata
import pandas as pd

# Configurar logging básico para monitorear el pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==========================================
# 1. CAPA DE EXTRACCIÓN (EXTRACT)
# ==========================================
def extraer_novedades_bases(cursor, databases):
    """Extrae y acumula las consultas de novedades desde múltiples bases de datos MySQL."""
    acumulado_novedades = []

    for db_name in databases:
        try:
            query = query_novedades(db_name)
            novedades_query = ejecutar_consulta_mysql(query, cursor)

            if not novedades_query:
                logging.warning(f"No se encontraron registros para la base de datos: {db_name}")
                continue

            acumulado_novedades.extend(novedades_query)
        except Exception as e:
            logging.error(f"Error al extraer datos de la base de datos {db_name}: {e}")

    if not acumulado_novedades:
        raise ValueError("El proceso de extracción no devolvió ningún registro de ninguna base de datos.")

    df_novedades = pd.DataFrame(acumulado_novedades)
    df_novedades.columns = [desc[0] for desc in cursor.description]
    return df_novedades


# ==========================================
# 2. CAPA DE TRANSFORMACIÓN (TRANSFORM)
# ==========================================
def limpiar_texto_general(texto):
    """Limpia tildes, caracteres especiales y convierte a mayúsculas."""
    if pd.isna(texto) or texto is None:
        return ""
    texto_str = str(texto).strip()
    texto_str = re.sub(r'[\r\n]+', ' ', texto_str)
    texto_nfd = unicodedata.normalize('NFD', texto_str)
    texto_sin_tildes = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    texto_limpio = re.sub(r'[^a-zA-Z0-9\s\-._]', '', texto_sin_tildes)
    return texto_limpio.upper()


def transformar_novedades(df_novedades, df_distribucion_redes, fe_reporte):
    """Aplica todas las reglas de limpieza, tipado, validación y mapeo geográfico."""
    df = df_novedades.copy()

    # Limpieza de Coordenadas
    df['longitud'] = df['longitud'].apply(limpiar_formato_longitud)
    df['latitud'] = df['latitud'].apply(limpiar_formato_latitud)

    # Limpieza masiva de campos de texto (tipo object) excluyendo coordenadas
    for col in df.select_dtypes(include=['object']).columns:
        if col not in ('longitud', 'latitud'):
            df[col] = df[col].apply(limpiar_texto_general)

    # Normalización de Fecha del registro
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')

    # Validación de coordenadas faltantes o inválidas
    df['validacion'] = ''
    mask_missing = df['longitud'].isna() | df['latitud'].isna()
    df.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (COORDENADAS INVALIDAS)'

    # Mapeo de Redes según Territorio
    if not df_distribucion_redes.empty:
        mapa_redes = df_distribucion_redes.set_index('TERRITORIO')['RED'].to_dict()
        df['redes'] = df['territorio'].map(mapa_redes).fillna('')
    else:
        df['redes'] = ''

    # Añadir fecha de reporte y limpiar formatos finales
    df['reporte_fecha'] = fe_reporte
    df = limpiar_formatos(df, columnas_fecha=['fecha', 'reporte_fecha'])

    return df


# ==========================================
# 3. CAPA DE CARGA (LOAD)
# ==========================================
def cargar_datos(df, client, sheet_id, fe_reporte):
    """Carga el DataFrame transformado hacia BigQuery y Google Sheets."""
    try:
        # Carga en BigQuery
        cargar_csv_a_bigquery(
            df,
            table_id="datos_aps.novedades",
            project_id="aps-project-478903"
        )

        # Actualización en Google Sheets
        sobrescribir_hoja(sheet_id, "cosolidado_novedades", df, client)

    except Exception as e:
        logging.error(f"Error crítico durante la fase de carga (Load): {e}")
        raise


# ==========================================
# 4. ORQUESTADOR PRINCIPAL DEL ETL
# ==========================================
def etl_novedades(cursor, df_distribucion_redes, fe_reporte, client, db, sheet_id):
    """Orquestador principal que ejecuta las tres fases del proceso ETL."""
    try:
        # Paso 1: Extract
        df_bruto = extraer_novedades_bases(cursor, db)

        # Paso 2: Transform
        df_transformado = transformar_novedades(df_bruto, df_distribucion_redes, fe_reporte)

        # Paso 3: Load
        cargar_datos(df_transformado, client, sheet_id, fe_reporte)

    except Exception as e:
        logging.error(f"El proceso ETL falló: {e}")
        raise