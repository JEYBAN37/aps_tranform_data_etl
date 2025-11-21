import logging
import re

import jaydebeapi
import pandas as pd
import folium
import requests
from matplotlib import pyplot as plt
import mysql.connector
from credenciales import DRIVER_MYSQL, MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DRIVER_PATH, \
    DATABASE_APS2024
from export_aps_124 import limpiar_formato_longitud, limpiar_formato_latitud
from mysql_conector import ejecutar_consulta_mysql
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point



ID_SOCIAMBIENTAL_LISTA = (
12981,
13001,
13001,
13004,
13004,
13004,
13081,
13086,
13086,
13103,
13113,
13117,
13118,
13120,
13130,
13162,
13171,
13175,
13208,
13216,
13217,
13218,
13220,
13228,
13251,
13262,
13285,
13285,
13289,
13295,
13301,
13303,
13307,
13308,
13313,
13317,
13322,
13329,
13333,
13337,
13349,
13354,
13391,
13404,
13409,
13464,
13471,
13474,
13485,
13496,
13502,
13514,
13519,
13538,
13568,
13579,
13580,
13581,
13582,
13588,
13602,
13605,
13608,
13608,
13611,
13612,
13613,
13615,
13618,
13622,
13622,
13624,
13631,
13651,
13790,
13792,
13978,
14000,
14003,
14002,
14007,
14012,
14014,
14039,
14045,
14046,
14051,
14052,
14054,
14144,
14148,
14150,
14153,
14158,
14163,
14165,
14168,
14173,
14179,
14186,
14187,
14191,
14194,
14198,
14199,
14201,
14202,
14205,
14206,
14207,
14208,
14270,
14271,
14274,
14275,
14277,
14278,
14279,
14280,
14282,
14288,
14305,
14307,
14314,
14325,
14345,
14427,
14429,
14430,
14433,
14468,
14470,
14474,
14478,
14503,
14505,
14509,
14511,
14516,
14519,
14663,
14689,
14736,
14827,
14769,
14888,
14894,
15051,
15062,
15069,
15073,
15076,
15263,
15266,
15274,
15273,
15275,
15277,
15278,
15279,
15281,
15265,
15282,
15286,
15288,
15291,
15295,
15297,
15300,
15302,
15304,
15408,
15411,
15412,
15417,
15420,
15422,
15425,
15427,
15429,
15431,
15450,
15451,
15467,
15471,
15472,
15474,
15475,
15456,
15513,
15571,
15596,
15622,
15720,
15727,
15729,
15747,
15760,
15767,
15772,
15771,
15775,
15780,
15784,
15783,
15792,
15795,
15803,
15813,
15815,
15824,
15829,
15831,
15965,
15968,
15977,
15981,
15982,
15990,
15996,
16010,
16028,
16037,
16043,
16047,
16050,
16055,
16061,
16159,
16164,
16175,
16181,
16189,
16203,
16204,
16210,
16219,
16227,
16231,
16258,
16287,
16366,
16370,
16372,
16373,
16385,
16393,
16394,
16398,
16406,
16534,
16537,
16567,
16569,
16571,
16574,
16575,
16578,
16636,
16638,
16639,
16695,
16717,
16720,
16727,
16770,
16771,
16776,
16782,
16783,
16788,
16790,
16792,
16796,
16797,
16801,
16805,
16808,
16809,
16812,
16905,
16906,
16908,
16909,
16912,
16914,
16917,
16923,
16927,
16955,
16972,
17077,
17268,
17275,
17277,
17279,
17285,
17536,
17537,
17543,
17545,
17553,
17703,
17704,
17706,
17707,
17709,
29063,
29906,
29934,
29948,
30024,
30047,
30141,
30144,
30373,
30513,
30599,
30838,
30840,
31049,
31051
)

def load_geojson_layer(layer_id: int):
    url = f"https://geoportal.pasto.gov.co/server/rest/services/Planeacion/Division_politico_administrativa/MapServer/{layer_id}/query?where=1%3D1&outFields=*&f=geojson"
    return requests.get(url).json()

def main():
    numero = limpiar_formato_longitud('772580399')
    numero_lat = limpiar_formato_latitud('1.222630')

    # Descargar límites administrativos del departamento de Nariño
    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2024,
        autocommit=False  # Disable autocommit
    )

    try:
        cursor = connection.cursor()
        acumulado_sociambiental = []
        personas_query = ejecutar_consulta_mysql(f"""
        SELECT 
        s.latitud,
        s.longitud
        FROM 
        agsolutic_aps2024.sociambientals s
        WHERE s.id IN {ID_SOCIAMBIENTAL_LISTA}
        """,cursor)
        acumulado_sociambiental.extend(personas_query)
        connection.commit()
        consolidado = pd.DataFrame(acumulado_sociambiental)
        consolidado.columns = [desc[0] for desc in cursor.description]

        consolidado.loc[:, 'latitud'] = consolidado['latitud'].apply(limpiar_formato_latitud)
        consolidado.loc[:, 'longitud'] = consolidado['longitud'].apply(limpiar_formato_longitud)


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

        mapa_pasto = folium.Map(location=[mapa_prueba["latitud"].mean(), mapa_prueba["longitud"].mean()], zoom_start=8)
        for _, row in mapa_prueba.iterrows():
            folium.Marker(
                location=[row["latitud"], row["longitud"]],
                popup=f"Lat: {row['latitud']}, Lon: {row['longitud']}"  # Mostrar coordenadas
            ).add_to(mapa_pasto)

        # Guardar el mapa como archivo HTML
        mapa_pasto.save("mapa_pasto.html")

        consolidado.to_csv('cv/consolidado_10_20_2025_limpio.csv', index=False)


        # Cargar comunas (urbano)
        geo_urbano = load_geojson_layer(2)

        # Cargar corregimientos (rural)
        geo_rural = load_geojson_layer(1)

        # Unir ambos en una sola lista de features
        full_geojson = {
            "type": "FeatureCollection",
            "features": geo_urbano["features"] + geo_rural["features"]
        }

        # Obtener centro del primer polígono para centrar el mapa
        first = full_geojson["features"][0]
        geom = first["geometry"]

        if geom["type"] == "Polygon":
            coords = geom["coordinates"][0]
        elif geom["type"] == "MultiPolygon":
            coords = geom["coordinates"][0][0]

        cent_lat = sum([c[1] for c in coords]) / len(coords)
        cent_lon = sum([c[0] for c in coords]) / len(coords)

        # Crear mapa
        m = folium.Map(location=[cent_lat, cent_lon], zoom_start=11)

        # Dibujar todo Pasto (urbano + rural)
        folium.GeoJson(
            full_geojson,
            name="División Pasto Completa",
            style_function=lambda f: {
                "fillColor": "blue" if f["properties"].get("NOM_COMUNA") else "green",
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.12
            }
        ).add_to(m)

        # ---- Tus coordenadas ----
        # Ejemplo (reemplaza con tu DataFrame real)
        # consolidado = pd.DataFrame({"latitud": [...], "longitud": [...]})

        gdf = gpd.GeoDataFrame(
            consolidado,
            geometry=gpd.points_from_xy(consolidado['longitud'], consolidado['latitud']),
            crs="EPSG:4326"
        )

        # GeoDataFrame de polígonos
        polygons = gpd.GeoDataFrame.from_features(full_geojson["features"])
        polygons = polygons.set_crs("EPSG:4326")

        # Spatial join para saber si está en Pasto
        join = gpd.sjoin(gdf, polygons, how="left", predicate="within")

        for _, row in join.iterrows():
            lat, lon = row["latitud"], row["longitud"]

            if pd.isna(row["index_right"]):
                color = "red"  # fuera de Pasto
            else:
                color = "blue" if row["geometry"].within(polygons.iloc[int(row["index_right"])].geometry) else "green"

            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color=color,
                fill=True,
                fill_opacity=0.9
            ).add_to(m)

        folium.LayerControl().add_to(m)

        # Guardar
        m.save("pasto_completo_urbano_rural.html")

    except Exception as e:
        logging.error(f"{e}")
        return

    finally:
        cursor.close()
        connection.close()
    # familias_query = ejecutar_consulta_mysql(QUERY_FILTRO_fAMILIAS, connection_mysql_replica, )

    # Registros de usuarios
    # tipo_1 = registro_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, FECHA_INICIAL, FECHA_FINAL, 124)



if __name__ == "__main__":
    main()