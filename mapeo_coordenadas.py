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
        cursor = connection.cursor(dictionary=True)

        # =========================================================================
        # CONFIGURACIÓN DE MÚLTIPLES TERRITORIOS
        # Agrega aquí todos los territorios que necesites renderizar en el mismo mapa
        # =========================================================================
        territorios_a_procesar = [
            {'name': 'T71', 'mc': 'Territorio15.1', 'nm': '15.1'},
            {'name': 'T51', 'mc': 'Territorio15.2', 'nm': '15.2'},
            {'name': 'T69', 'mc': 'Territorio15.3', 'nm': '15.3'},
        ]

        # Extraemos los nombres de territorios para la consulta SQL dinámica IN (...)
        lista_nombres_sql = ", ".join([f"'{t['name']}'" for t in territorios_a_procesar])

        # Consulta SQL modificada para traer TODOS los territorios configurados de una sola vez
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
        WHERE u.territorio IN ({lista_nombres_sql}) AND s.latitud IS NOT NULL
        """
        cursor.execute(sql)
        todos_los_puntos = cursor.fetchall()

        # Variables para calcular el centro dinámico del mapa
        centro_lat, centro_lon = 1.2136, -77.2811  # Valores por defecto (Pasto)

        if todos_los_puntos:
            lats = []
            lons = []
            for fila in todos_los_puntos:
                try:
                    val_lat = float(str(fila['latitud']).strip().replace(" ", ""))
                    val_lon = float(str(fila['longitud']).strip().replace(" ", ""))

                    # Validación de rango para Pasto
                    if (0 < val_lat < 2) and (-78 < val_lon < -76):
                        lats.append(val_lat)
                        lons.append(val_lon)
                except:
                    continue

            if lats and lons:
                centro_lat = sum(lats) / len(lats)
                centro_lon = sum(lons) / len(lons)

        print(f"📍 Centro calculado para todos los territorios: {centro_lat}, {centro_lon}")

        # 1. Crear el mapa base
        m = folium.Map(location=[centro_lat, centro_lon], zoom_start=15)
        folium.TileLayer('OpenStreetMap', name='Mapa Normal (Calles)').add_to(m)
        folium.TileLayer('cartodbpositron', name='Mapa Gris (Limpio)').add_to(m)
        folium.TileLayer(
            tiles='https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',
            attr='Google',
            name='Google Satélite',
            overlay=False,
            control=True
        ).add_to(m)

        # 2. CARGAR KML Y PROCESAR CAPAS POR CADA TERRITORIO
        fiona.drvsupport.supported_drivers['KML'] = 'rw'
        ruta_kml = "mapa/aps_oriente.kml"
        capas = fiona.listlayers(ruta_kml)

        # Paleta de colores para diferenciar visualmente los territorios en el mapa
        colores = ['#8000ff', '#ff5733', '#33ff57', '#3357ff', '#f3ff33', '#ff33f3']

        # Iteramos sobre cada territorio configurado para filtrar su KML
        for index, terr in enumerate(territorios_a_procesar):
            gdfs_lista = []
            color_actual = colores[index % len(colores)]  # Cicla colores si hay más territorios que colores

            for capa in capas:
                data_capa = gpd.read_file(ruta_kml, driver='KML', layer=capa)

                # Filtro dinámico con los parámetros del territorio actual del bucle
                filtro_territorio = data_capa[
                    data_capa['Description'].astype(str).str.contains(f"Territorio: {terr['mc']}", case=False,
                                                                      na=False) |
                    data_capa['Name'].astype(str).str.contains(terr['nm'], case=False, na=False)
                    ]

                if not filtro_territorio.empty:
                    gdfs_lista.append(filtro_territorio)

            if gdfs_lista:
                gdf_final = pd.concat(gdfs_lista, ignore_index=True)

                if gdf_final.crs is None:
                    gdf_final.set_crs(epsg=4326, inplace=True)
                else:
                    gdf_final = gdf_final.to_crs(epsg=4326)

                # Añadimos la capa poligonal del territorio al mapa
                folium.GeoJson(
                    gdf_final,
                    name=f"Polígono: {terr['name']} ({terr['nm']})",
                    style_function=lambda f, col=color_actual: {
                        "fillColor": col,
                        "color": "black",
                        "weight": 2.5,
                        "fillOpacity": 0.2
                    },
                    tooltip=folium.GeoJsonTooltip(fields=['Name'], aliases=[f"Sector {terr['nm']}:"])
                ).add_to(m)
                print(f"✅ Territorio {terr['name']} ({terr['nm']}) filtrado y dibujado.")
            else:
                print(f"⚠️ No se encontró geometría KML para {terr['name']} ({terr['nm']}).")

        # 3. DIBUJAR LOS PUNTOS DE LA BASE DE DATOS
        puntos_dibujados = 0
        for fila in todos_los_puntos:
            try:
                lat = float(str(fila['latitud']).strip().replace(" ", ""))
                lon = float(str(fila['longitud']).strip().replace(" ", ""))

                street_url = f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}"
                raw_phone = fila.get('celular', '')
                sanitized_phone = ''.join(c for c in str(raw_phone) if c.isdigit() or c == '+')
                tel_url = f"tel:{sanitized_phone}" if sanitized_phone else ""

                popup_html = (
                    f"<b>Territorio:</b> {fila['territorio']}<br>"
                    f"<b>Micro:</b> {fila['microterritorio']}<br>"
                    f"<b>Familia ID:</b> {fila['id']}<br>"
                    f"<b>Tel:</b> <a href=\"{tel_url}\">{fila.get('celular', '')}</a><br>"
                    f"<a href=\"{street_url}\" target=\"_blank\" rel=\"noopener noreferrer\">Open Street View</a>"
                )

                if lat and lon:
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=4,
                        color='white',
                        weight=1,
                        fill=True,
                        fill_color='red',  # Puedes cambiar este color si quieres diferenciar puntos por territorio
                        fill_opacity=0.9,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(m)
                    puntos_dibujados += 1

            except (ValueError, TypeError):
                continue

        # 4. Guardar archivo final unificado
        folium.LayerControl().add_to(m)

        # Generamos un nombre dinámico basado en los territorios cargados
        nombre_salida = "_".join([t['name'] for t in territorios_a_procesar])
        m.save(f"mapa/aps_mapa_MULTIPLE_{nombre_salida}.html")
        print(f"✅ ¡Mapa unificado listo! Se cargaron {puntos_dibujados} puntos en total.")

    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Error general: {e}")
    finally:
        if cursor: cursor.close()
        if connection: connection.close()


if __name__ == "__main__":
    main()
