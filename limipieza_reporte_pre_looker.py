import logging
import re

import jaydebeapi
import pandas as pd
import folium
from matplotlib import pyplot as plt

from credenciales import DRIVER_MYSQL, MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DRIVER_PATH
from export_aps_124 import limpiar_formato_longitud, limpiar_formato_latitud
from mysql_conector import ejecutar_consulta_mysql
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point

def main():
    numero = limpiar_formato_longitud('772580399')
    numero_lat = limpiar_formato_latitud('1.222630')

    # Descargar límites administrativos del departamento de Nariño
    conexion_replica = jaydebeapi.connect(
        DRIVER_MYSQL,
        MYSQL_APS,
        [MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD],
        DRIVER_PATH
    )

    connection_mysql_replica = conexion_replica.cursor()

    consolidado = pd.read_csv('cv/consolidado_10_20_2025-1760981722625.csv', dtype=str)

    try:

        consolidado = pd.DataFrame([{
            'id_familia': row['id_familia'],
            'id_sociambiental': row['sociambiental_id'],
            'fecha' : row['fecha'],
            'direccion' : row['direccion'],
            'apellidosfamilia' : row['apellidosfamilia'],
            'microterritorio' : row['microterritorio'],
            'cod_microterritorio' : row['cod_microterritorio'],
            'comuna': row['comuna'],
            'terriorio': row['territorio'],
            'zona' : row['zona'],
            'profesional_ebs' : row['nombres'],
            'tipodocumento' : row['tipodocumento'],
            'primerapellido': row['primerapellido'],
            'segundoapellido': row['segundoapellido'],
            'primernombre': row['primernombre'],
            'segundonombre': row['segundonombre'],
            'aps': row['aseguradora'],
            'regimen' : row['regimen'],
            'cedula': str(re.sub(r'[-.#_·|/Ñ\s],', '',row['numerodoc'] )  ) if pd.notna(row['numerodoc']) else '',
            'latitud': limpiar_formato_latitud(row['latitud']),
            'longitud': limpiar_formato_longitud(row['longitud']),
        } for _, row in consolidado.iterrows()
        ])


        pd_ubicaciones_erroneas = consolidado[(consolidado['latitud'].isnull()) | (consolidado['longitud'].isnull())]

        consolidado = consolidado.drop(pd_ubicaciones_erroneas.index)

        # Descargar el polígono del municipio de Pasto
        pasto_full = ox.features_from_place(
            "Pasto, Nariño, Colombia",
            tags={"boundary": "administrative"}
        )

        # Filtrar solo el municipio (admin_level 6 o 7, nombre exacto)
        pasto_municipio = pasto_full[
            (pasto_full["admin_level"].isin(["6", "7"])) &
            (pasto_full["name"].str.lower() == "pasto")
            ]

        # Si hay varios resultados, unificarlos en un solo polígono
        pasto_polygon = pasto_municipio.union_all()

        # Convertir tu DataFrame con coordenadas en GeoDataFrame
        gdf = gpd.GeoDataFrame(
            consolidado,
            geometry=gpd.points_from_xy(consolidado['longitud'], consolidado['latitud']),
            crs="EPSG:4326"
        )

        # Crear un mapa centrado en el municipio de Pasto
        mapa_pasto = folium.Map(location=[pasto_polygon.centroid.y, pasto_polygon.centroid.x], zoom_start=12)

        # Añadir el polígono del municipio al mapa
        folium.GeoJson(
            pasto_polygon,
            style_function=lambda x: {"fillColor": "blue", "color": "blue", "fillOpacity": 0.2},  # Más opaco
        ).add_to(mapa_pasto)

        gdf["en_pasto"] = gdf.within(pasto_polygon)

        # Filtrar los que están fuera del municipio
        fuera = gdf[~gdf["en_pasto"]]
        mapa_prueba = fuera.drop_duplicates(subset=['latitud', 'longitud'])

        mapa = folium.Map(location=[mapa_prueba["latitud"].mean(), mapa_prueba["longitud"].mean()], zoom_start=8)
        for _, row in mapa_prueba.iterrows():
            folium.Marker(
                location=[row["latitud"], row["longitud"]],
                popup=f"Lat: {row['latitud']}, Lon: {row['longitud']}"  # Mostrar coordenadas
            ).add_to(mapa)

        # Guardar el mapa como archivo HTML
        mapa_pasto.save("mapa_pasto.html")

        consolidado.to_csv('cv/consolidado_10_20_2025_limpio.csv', index=False)

    except Exception as e:
        logging.error(f"{e}")
        return

    finally:
        connection_mysql_replica.close()
        conexion_replica.close()

    # familias_query = ejecutar_consulta_mysql(QUERY_FILTRO_fAMILIAS, connection_mysql_replica, )

    # Registros de usuarios
    # tipo_1 = registro_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, FECHA_INICIAL, FECHA_FINAL, 124)



if __name__ == "__main__":
    main()