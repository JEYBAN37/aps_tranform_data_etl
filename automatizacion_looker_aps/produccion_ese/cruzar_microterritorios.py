import re
import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Habilitar lectura de KML
fiona.drvsupport.supported_drivers['KML'] = 'rw'


def cargar_kml_aps(ruta_kml):
  """Carga el KML de la estrategia APS en un GeoDataFrame WGS84."""
  capas = fiona.listlayers(ruta_kml)
  gdfs = [gpd.read_file(ruta_kml, driver='KML', layer=c) for c in capas]
  gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

  if gdf.crs is None:
    gdf.set_crs(epsg=4326, inplace=True)
  else:
    gdf = gdf.to_crs(epsg=4326)

  return gdf


def extraer_codigo_territorio(texto):
  """Busca patrones como T65, T68, T-65 o 'TERRITORIO 65' en el texto del KML."""
  if not texto or pd.isna(texto):
    return None

  texto_str = str(texto).upper()

  # 1. Coincidencia directa 'T65'
  match = re.search(r'\bT\d{1,3}\b', texto_str)
  if match:
    return match.group(0)

  # 2. Coincidencia 'T-65' -> 'T65'
  match_sep = re.search(r'\bT[-\s]?(\d{1,3})\b', texto_str)
  if match_sep:
    return f'T{match_sep.group(1)}'

  # 3. Coincidencia 'TERRITORIO 65' -> 'T65'
  match_terr = re.search(r'TERRITORIO\s*(\d{1,3})', texto_str)
  if match_terr:
    return f'T{match_terr.group(1)}'

  return None


def obtener_territorio_por_barrio_overpass(
    palabra_clave,
    ruta_geojson=r'C:\Users\acer\Desktop\Esteban Trabajo\APS\mapa\pasto.geojson',
    ruta_kml=r'C:\Users\acer\Desktop\Esteban Trabajo\APS\mapa\aps.kml',
):
  """Flujo completo:

  1. Busca 'palabra_clave' en el GeoJSON de Overpass Turbo.
  2. Obtiene el centroide del barrio en Pasto.
  3. Realiza el Spatial Join contra el KML de APS.
  4. Retorna la nomenclatura 'TXX' (ej. T65).
  """
  # 1. Cargar fuentes de mapas
  gdf_barrios = gpd.read_file(ruta_geojson)
  gdf_aps = cargar_kml_aps(ruta_kml)

  # 2. Asegurar proyecciones en WGS84
  if gdf_barrios.crs is None or gdf_barrios.crs.to_epsg() != 4326:
    gdf_barrios = gdf_barrios.to_crs(epsg=4326)

  # 3. Buscar la palabra clave en las columnas de nombre del GeoJSON (name, suburb, etc.)
  cols_texto = [
      c
      for c in gdf_barrios.columns
      if c.lower() in ['name', 'suburb', 'official_name']
  ]
  if not cols_texto:
    cols_texto = gdf_barrios.select_dtypes(include=['object']).columns

  condicion = pd.Series(False, index=gdf_barrios.index)
  for col in cols_texto:
    condicion |= gdf_barrios[col].astype(str).str.contains(
        palabra_clave, case=False, na=False
    )

  barrio_match = gdf_barrios[condicion]

  if barrio_match.empty:
    return {
        'exito': False,
        'mensaje': (
            f"❌ No se encontró el barrio '{palabra_clave}' en el GeoJSON de"
            ' Pasto.'
        ),
    }

  # 4. Obtener centroide del barrio encontrado
  poligono = barrio_match.geometry.iloc[0]
  centroide = poligono.centroid
  lat, lon = centroide.y, centroide.x

  nombre_oficial = barrio_match.iloc[0].get('name', palabra_clave)

  # 5. Cruce Espacial (Point-in-Polygon)
  punto = Point(lon, lat)
  cruce_aps = gdf_aps[gdf_aps.contains(punto)]

  if cruce_aps.empty:
    return {
        'exito': True,
        'barrio': nombre_oficial,
        'coordenadas': (lat, lon),
        'territorio_codigo': 'SIN_COBERTURA_KML',
    }

  # 6. Decodificar la nomenclatura TXX buscando en los campos del KML
  registro_kml = cruce_aps.iloc[0]
  codigo_t = None

  territorio = registro_kml['Territorio']


  return {
      'exito': True,
      'barrio': territorio,
      'coordenadas': {'lat': lat, 'lon': lon},
      'territorio_codigo': codigo_t or 'T_DESCONOCIDO',  # Retorna ej: 'T65'
  }


# ==========================================
# 🧪 EJEMPLO DE PRUEBA
# ==========================================
if __name__ == '__main__':
  # Reemplaza por la palabra clave del barrio que quieres consultar
  busqueda = 'VILLAFLOR'

  res = obtener_territorio_por_barrio_overpass(busqueda)

  if res['exito']:
    print('\n✅ TRADUCCIÓN COMPLETADA:')
    print(f"📍 Barrio Encontrado: {res['barrio']}")
    print(
        f"🌐 Coordenadas: Lat {res['coordenadas']['lat']:.5f}, Lon"
        f" {res['coordenadas']['lon']:.5f}"
    )
    print(
        f"🏷️  Nomenclatura Territorio: {res['territorio_codigo']}"
    )  # Muestra ej: T65
  else:
    print(res['mensaje'])