from contextlib import nullcontext
import re
import unicodedata

import fiona
import geopandas as gpd
import mysql.connector
import pandas as pd
from rapidfuzz import fuzz, process
from shapely.geometry import Point

from credenciales import (
    DATABASE_APS2025,
    MYSQL_APS,
    MYSQL_REPLICA_PASSWORD,
    MYSQL_REPLICA_USER,
)
from mysql_conector import ejecutar_consulta_mysql

# Habilitar lectura de KML
fiona.drvsupport.supported_drivers["KML"] = "rw"


def cargar_kml_aps(ruta_kml):
    """Carga el KML de la estrategia APS en un GeoDataFrame WGS84."""
    capas = fiona.listlayers(ruta_kml)
    gdfs = [gpd.read_file(ruta_kml, driver="KML", layer=c) for c in capas]
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    else:
        gdf = gdf.to_crs(epsg=4326)

    return gdf


def cargar_fuentes_espaciales(
        ruta_geojson=r"F:\APS AUTOMATIZACIONES\aps\mapa\pasto.geojson",
        ruta_kml=r"F:\APS AUTOMATIZACIONES\aps\mapa\aps.kml",
):
    print("📂 Cargando fuentes de mapas en memoria...")
    gdf_barrios = gpd.read_file(ruta_geojson)
    gdf_aps = cargar_kml_aps(ruta_kml)

    if gdf_barrios.crs is None or gdf_barrios.crs.to_epsg() != 4326:
        gdf_barrios = gdf_barrios.to_crs(epsg=4326)

    if gdf_aps.crs is None or gdf_aps.crs.to_epsg() != 4326:
        gdf_aps = gdf_aps.to_crs(epsg=4326)

    cols_texto = [
        c
        for c in gdf_barrios.columns
        if c.lower() in ["name", "suburb", "official_name"]
    ]
    if not cols_texto:
        cols_texto = gdf_barrios.select_dtypes(include=["object"]).columns

    for col in cols_texto:
        gdf_barrios[f"{col}_clean"] = (
            gdf_barrios[col].astype(str).str.lower().str.replace(" ", "")
        )

    return gdf_barrios, gdf_aps, cols_texto


def extraer_barrio_direccion(direccion):
    """Limpieza radical: elimina vías, nomenclaturas, números, manzanas y casas."""
    if not isinstance(direccion, str) or not direccion.strip():
        return ""

    # 1. Quitar acentos y caracteres raros (ej: NÂº -> NRO)
    texto = unicodedata.normalize("NFKD", direccion)
    texto = "".join([c for c in texto if not unicodedata.combining(c)])
    texto = texto.upper().strip()

    # 2. Normalizar abreviaturas de número
    texto = re.sub(r"N[ÂA]º|Nº|N°|NO\b", "NRO", texto)

    # 3. Eliminar prefijos de zonas, barrios, sectores o corregimientos
    texto = re.sub(
        r"^(B\s*//|B/|BARRI|BARRIO|CORR|CTO|CGTO/\.|CGTO|CORREGIMIENTO|VDA|VRA|VEREDA|SECTOR)\s*",
        "",
        texto,
    )

    # 4. Eliminar vías completas con números al inicio
    texto = re.sub(
        r"^(CLL|CALLE|CRA|CARRERA|CARR|CARRE|KRA|CR|TRANSVERSAL|TV|DIAGONAL|DG|DIG|AV|AVENIDA)\s*\d+.*?\d+\s*",
        "",
        texto,
        flags=re.IGNORECASE,
    )

    # 5. Eliminar secuencias de Manzana, Casa, Lote, Torre, Apto
    texto = re.sub(
        r"\b(MZ|MZA|MAZ|MC|MANZANA|M)\s*[\w\d]+\s*(,|/)?\s*(CS|CASA|LOTE|LT|APTO|APT|TORRE|TR)?\s*[\w\d]*\b",
        "",
        texto,
        flags=re.IGNORECASE,
    )

    # 6. Dejar únicamente texto alfabético y limpiar espacios sobrantes
    texto = re.sub(r"[^A-Z\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto if len(texto) > 2 else ""


def obtener_territorio_optimizado(
        direccion,
        gdf_barrios,
        gdf_aps,
        cols_texto,
        df_microterritorios,
        barrios_ordenados,
):
    """Algoritmo CASCADA: Directo -> Regex + Fuzzy Matching -> Cruce Espacial."""
    if (
            direccion is None
            or pd.isna(direccion)
            or not isinstance(direccion, str)
            or not direccion.strip()
    ):
        return "SIN_DIRECCION", "SIN_DIRECCION"

    dir_upper = direccion.upper().strip()
    barrio_encontrado = None

    # ==============================================================================
    # NIVEL 1: Coincidencia Directa por Lista de Barrios Ordenada por Longitud
    # ==============================================================================
    for barrio in barrios_ordenados:
        barrio_clean = barrio.upper().strip()
        if len(barrio_clean) >= 3 and barrio_clean in dir_upper:
            barrio_encontrado = barrio
            break

    # ==============================================================================
    # NIVEL 2: Limpieza Profunda + Fuzzy Matching (Tolerancia a errores de tipeo)
    # ==============================================================================
    if not barrio_encontrado:
        palabra_limpia = extraer_barrio_direccion(direccion)
        if palabra_limpia:
            # Búsqueda difusa para capturar errores como "SNTE FE" o "CUBIJAN BAJ"
            match_fuzzy = process.extractOne(
                palabra_limpia,
                barrios_ordenados,
                scorer=fuzz.partial_ratio,
                score_cutoff=82,
            )
            if match_fuzzy:
                barrio_encontrado = match_fuzzy[0]

    if not barrio_encontrado:
        return "SIN_COBERTURA_BARRIO", "SIN_COBERTURA_BARRIO"

    # ==============================================================================
    # NIVEL 3: Cruce Geográfico (Centroide del Barrio en KML APS)
    # ==============================================================================
    barrio_norm_target = str(barrio_encontrado).lower().replace(" ", "")
    condicion_final = pd.Series(False, index=gdf_barrios.index)

    for col in cols_texto:
        condicion_final |= (
                gdf_barrios[f"{col}_clean"] == barrio_norm_target
        )

    barrio_match_final = gdf_barrios[condicion_final]

    if barrio_match_final.empty:
        return "SIN_COBERTURA_GEOJSON", "SIN_COBERTURA_GEOJSON"

    # Extraer centroide y validar contra polígonos del KML APS
    poligono = barrio_match_final.geometry.iloc[0]
    centroide = poligono.centroid
    cruce_aps = gdf_aps[gdf_aps.contains(centroide)]

    if cruce_aps.empty:
        return "SIN_COBERTURA_KML", "SIN_COBERTURA_KML"

    # Extraer datos KML de la columna Description
    registro_kml = cruce_aps.iloc[0]
    extraccion = str(registro_kml.get("Description", "")).split("<br>")

    territorio_raw = (
        extraccion[7].split(":")[-1].strip()
        if len(extraccion) > 7
        else "T_DESCONOCIDO"
    ).replace(" ", "")

    microterritorio_num = (
        extraccion[9].split(":")[-1].strip()
        if len(extraccion) > 9
        else "MT_DESCONOCIDO"
    ).replace(" ", "")

    # Mapear el código T desde la tabla de MySQL
    match_codigo = df_microterritorios[
        df_microterritorios["territorio_extraido"] == territorio_raw
        ]["territorio_codigo"].values

    territorio = df_microterritorios[
        (df_microterritorios["territorio_extraido"] == territorio_raw)
    ][["microterritorio", "codigo"]]

    territorio_cod = (
        match_codigo[0]
        if len(match_codigo) > 0
        else (territorio_raw or "T_DESCONOCIDO")
    )

    territorio['codigo'] = territorio['codigo'].astype(str).str[1:]
    nombre = territorio[territorio['codigo'] == microterritorio_num]['microterritorio']
    print(microterritorio_num, territorio)
    # Se valida que exista microterritorio_num Y que la consulta haya encontrado coincidencias
    micro_cod = str(nombre.iloc[0]) if (microterritorio_num and not nombre.empty) else microterritorio_num

    return territorio_cod, micro_cod


# ==========================================
# 🧪 EJECUCIÓN PRINCIPAL
# ==========================================
if __name__ == "__main__":
    try:
        connection = mysql.connector.connect(
            host=MYSQL_APS,
            user=MYSQL_REPLICA_USER,
            password=MYSQL_REPLICA_PASSWORD,
            database=DATABASE_APS2025,
            autocommit=False,
        )
        cursor = connection.cursor()
        microterritorios = ejecutar_consulta_mysql(
            f"SELECT u.microterritorio, u.cod_microterritorio FROM {DATABASE_APS2025}.ubicaciones u",
            cursor,
        )
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        exit(1)

    # Preprocesamiento de la tabla de microterritorios
    df_microterritorios = pd.DataFrame(microterritorios)

    df_microterritorios.columns = ["microterritorio", "codigo"]

    df_microterritorios["territorio_extraido"] = (
        df_microterritorios.iloc[:, 0]
        .str.extract(r"\b(Territorio.*)", flags=re.IGNORECASE)[0]
        .str.replace(" ", "", regex=False)
    )

    df_microterritorios["territorio_codigo"] = df_microterritorios.iloc[
                                               :, 0
                                               ].str.extract(r"\b(T\d+)\b", flags=re.IGNORECASE)[0]

    # Carga de mapas espaciales
    gdf_barrios, gdf_aps, cols_texto = cargar_fuentes_espaciales()

    # Construcción de la lista unificada de búsqueda (Barrios + Veredas/Corregimientos)
    lista_sectores = []
    for col in cols_texto:
        lista_sectores.extend(gdf_barrios[col].dropna().tolist())

    zonas_rurales = [
        "OBONUCO",
        "CUJACAL",
        "CUBIJAN",
        "JONGOVITO",
        "MOCONDINO",
        "MAPACHICO",
        "CATAMBUCO",
        "ENCANO",
        "EL ENCANO",
        "BUESAQUILLO",
        "SANTA BARBARA",
        "LA LAGUNA",
        "SOCORRO",
        "PUERRES",
    ]
    lista_sectores.extend(zonas_rurales)

    # Limpieza y orden de mayor a menor longitud
    barrios_oficiales_unicos = list(
        set([
            str(b).strip().upper()
            for b in lista_sectores
            if len(str(b).strip()) > 2
        ])
    )
    barrios_ordenados = sorted(barrios_oficiales_unicos, key=len, reverse=True)

    # Carga del dataset de producción
    print("📄 Cargando archivo CSV de producción...")
    df_personas = pd.read_csv(r"F:\APS AUTOMATIZACIONES\aps\automatizacion_looker_aps\produccion_ese\bases_downloads\produccion_filtrada_2026_EBS_agosto.csv", encoding='latin-1')

    df_caracterizados = pd.read_csv(
        r"F:\APS AUTOMATIZACIONES\reportes\2026-10-26\looker\cosolidado_personas_2026-10-26.csv")

    # 1. Asegurar que la columna de identificación en df_personas esté limpia y en formato texto
    df_personas["Identificacion"] = df_personas["Identificacion"].astype(str).str.strip().str.strip('.0')
    df_caracterizados["doc_id"] = df_caracterizados["doc_id"].astype(str).str.strip().str.strip('.0')

    # 2. Seleccionar únicamente las columnas necesarias de df_caracterizados
    # (Verificando que 'territorio' y 'microterritorio' existan en ese archivo)
    columnas_a_cruzar = ["doc_id"]
    if "territorio" in df_caracterizados.columns:
        columnas_a_cruzar.append("territorio")
    if "microterritorio" in df_caracterizados.columns:
        columnas_a_cruzar.append("microterritorio")

    df_temp = df_caracterizados[columnas_a_cruzar].drop_duplicates(subset=["doc_id"])

    # 3. Realizar el merge correctamente utilizando left join
    df_personas = df_personas.merge(
        df_temp,
        left_on="Identificacion",
        right_on="doc_id",
        how="left",
        suffixes=("", "_carat")  # Evita conflictos si las columnas ya existían
    )

    # Si el archivo origen ya traía una columna territorio vacía o antigua, la rellenamos con la del merge
    if "territorio_carat" in df_personas.columns:
        df_personas["territorio"] = df_personas["territorio"].fillna(df_personas["territorio_carat"])
        df_personas.drop(columns=["territorio_carat"], inplace=True)

    if "microterritorio_carat" in df_personas.columns:
        df_personas["microterritorio"] = df_personas["microterritorio"].fillna(df_personas["microterritorio_carat"])
        df_personas.drop(columns=["microterritorio_carat"], inplace=True)

    # 4. Continuar con tu lógica de máscara
    mask_sin_micro = df_personas["microterritorio"].isna()
    total_procesar = mask_sin_micro.sum()

    direcciones_unicas = (
        df_personas.loc[mask_sin_micro, "Dir_Afil"]
        .dropna()
        .astype(str)
        .unique()
    )

    print(
        f"\n🔄 Procesando {len(direcciones_unicas)} direcciones únicas (equivalentes a {total_procesar} registros)..."
    )

    # Evaluación masiva sobre direcciones únicas
    diccionario_resultados = {
        d: obtener_territorio_optimizado(
            d,
            gdf_barrios,
            gdf_aps,
            cols_texto,
            df_microterritorios,
            barrios_ordenados,
        )
        for d in direcciones_unicas
    }

    # Mapeo a las 200.000+ filas
    res_tuplas = df_personas.loc[mask_sin_micro, "Dir_Afil"].map(
        diccionario_resultados
    )

    # Desempaquetado seguro en las dos columnas
    df_personas.loc[mask_sin_micro, "territorio"] = [
        t[0] if isinstance(t, tuple) else "SIN_DATOS" for t in res_tuplas
    ]
    df_personas.loc[mask_sin_micro, "microterritorio"] = [
        t[1] if isinstance(t, tuple) else "SIN_DATOS" for t in res_tuplas
    ]

    # Exportar resultados
    df_personas.to_csv(
        "bases_downloads/produccion_filtrada_2026_EBS_microterritorios_para_eps.csv",
        index=False,
    )

    print("🎉 ¡Proceso finalizado con éxito!")
