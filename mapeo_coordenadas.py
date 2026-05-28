import folium
import geopandas as gpd
import fiona
import mysql.connector
import pandas as pd

from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025
from mysql_conector import ejecutar_consulta_mysql


def main():
    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False
    )
    try:
        cursor = connection.cursor(dictionary=True)  # Usar dictionary=True facilita el manejo
        territorio_name = 'T07'
        territori_mc = 'Territorio 3.1'
        territori_nm = '3.1'
        # Consulta SQL
        sql = f"""
        SELECT 
            u.microterritorio,
            u.territorio,
            s.latitud,
            s.longitud,
            f.apellidos,
            f.celular,
            f.id
        FROM {DATABASE_APS2025}.familias f
        LEFT JOIN {DATABASE_APS2025}.sociambientals s ON s.id = f.sociambiental_id
        LEFT JOIN {DATABASE_APS2025}.ubicaciones u ON u.id = s.ubicacion_id
        WHERE u.territorio = '{territorio_name}' AND s.latitud IS NOT NULL
        """
        cursor.execute(sql)
        territorio = cursor.fetchall()
        centro_lat, centro_lon = 1.2136, -77.2811  # Valores por defecto (Pasto)

        if territorio:
            lats = []
            lons = []
            for fila in territorio:
                try:
                    # Limpiamos y convertimos
                    val_lat = float(str(fila['latitud']).strip().replace(" ", ""))
                    val_lon = float(str(fila['longitud']).strip().replace(" ", ""))

                    # VALIDACIÓN DE RANGO: Solo agregar si son coordenadas reales de Pasto
                    # Pasto está entre Lat: 1.0 y 1.5 / Lon: -77.0 y -77.5
                    if (0 < val_lat < 2) and (-78 < val_lon < -76):
                        lats.append(val_lat)
                        lons.append(val_lon)
                except:
                    continue

            # Calculamos el promedio real
            if lats and lons:
                centro_lat = sum(lats) / len(lats)
                centro_lon = sum(lons) / len(lons)

        # Imprime para verificar en consola
        print(f"📍 Centro calculado: {centro_lat}, {centro_lon}")

        # 1. Crear el mapa base
        # CAMBIO: zoom_start máximo 16 para que se vea la ciudad
        m = folium.Map(location=[centro_lat, centro_lon], zoom_start=17)
        folium.TileLayer('OpenStreetMap', name='Mapa Normal (Calles)').add_to(m)
        folium.TileLayer('cartodbpositron', name='Mapa Gris (Limpio)').add_to(m)
        folium.TileLayer(
            tiles='https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',
            attr='Google',
            name='Google Satélite',
            overlay=False,
            control=True
        ).add_to(m)

        # 2. CARGAR Y FILTRAR ÚNICAMENTE EL TERRITORIO 3.1
        fiona.drvsupport.supported_drivers['KML'] = 'rw'
        ruta_kml = "mapa/aps_oriente.kml"

        capas = fiona.listlayers(ruta_kml)
        gdfs_lista = []

        for capa in capas:
            data_capa = gpd.read_file(ruta_kml, driver='KML', layer=capa)

            # FILTRO CRÍTICO:
            # Buscamos en la columna 'Description' o 'Name' el texto "3.1"
            # Usamos una expresión regular para que sea exacto y no traiga el "3.11" por error
            filtro_31 = data_capa[
                data_capa['Description'].astype(str).str.contains(f'Territorio: {territori_mc}', case=False, na=False) |
                data_capa['Name'].astype(str).str.contains(territori_nm, case=False, na=False)
                ]

            if not filtro_31.empty:
                gdfs_lista.append(filtro_31)

        if gdfs_lista:
            gdf_final = pd.concat(gdfs_lista, ignore_index=True)

            # Estandarizar coordenadas
            if gdf_final.crs is None:
                gdf_final.set_crs(epsg=4326, inplace=True)
            else:
                gdf_final = gdf_final.to_crs(epsg=4326)

            # DIBUJAR EN EL MAPA
            folium.GeoJson(
                gdf_final,
                name=f"{territorio_name} Seleccionado",
                style_function=lambda f: {
                    "fillColor": "#8000ff",  # Color Púrpura para identificarlo rápido
                    "color": "black",  # Borde negro
                    "weight": 2.5,  # Borde más grueso
                    "fillOpacity": 0.2  # Más visible
                },
                tooltip=folium.GeoJsonTooltip(fields=['Name'], aliases=['Sector 3.1:'])
            ).add_to(m)

            print("✅ El Territorio 3.1 ha sido filtrado y dibujado.")
        else:
            print(f"⚠️ No se encontró ningún dato que coincida con {territorio_name} en el KML. Verifica los nombres y el contenido del KML.")

        # 3. DIBUJAR LOS PUNTOS DE LA BASE DE DATOS (MICRO-DIVISIONES)
        # Aquí es donde mapeamos cada familia/ubicación de la DB
        for fila in territorio:
            try:
                # Limpieza de espacios y conversión segura
                # .strip() quita espacios al inicio/final, .replace(" ", "") quita espacios internos
                lat = float(str(fila['latitud']).strip().replace(" ", ""))
                lon = float(str(fila['longitud']).strip().replace(" ", ""))

                street_url = f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}"
                raw_phone = fila.get('celular', '')
                sanitized_phone = ''.join(c for c in str(raw_phone) if c.isdigit() or c == '+')
                tel_url = f"tel:{sanitized_phone}" if sanitized_phone else ""
                print(fila)
                popup_html = (
                    f"Micro: {fila['microterritorio']}<br>"
                    f"Familia: {fila['id']}<br>"
                    f"Tel: <a href=\"{tel_url}\">{fila.get('celular', '')}</a><br>"
                    f"<a href=\"{street_url}\" target=\"_blank\" rel=\"noopener noreferrer\">Open Street View</a>"
                )

                # Solo dibujar si las coordenadas son válidas
                if lat and lon:
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=4,
                        color='white',  # Borde blanco para contraste
                        weight=1,  # Grosor del borde
                        fill=True,
                        fill_color='red',  # Interior rojo
                        fill_opacity=0.9,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(m)

            except (ValueError, TypeError):
                # Si la coordenada está mal escrita o es un texto extraño, salta a la siguiente
                print(f"⚠️ Coordenada ignorada por error de formato: Lat {fila['latitud']}, Lon {fila['longitud']}")
                continue

        # 4. Guardar
        folium.LayerControl().add_to(m)
        m.save(f"mapa/aps_mapa_{territorio_name}.html")
        print(f"✅ ¡Mapa listo! Se cargaron {len(territorio)} puntos sobre los sectores del KML.")

    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Error: {e}")
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

if __name__ == "__main__":
    main()