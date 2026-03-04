import os
import mysql.connector
import pandas as pd
from google.oauth2.service_account import Credentials
from automatizacion_looker_aps.familias.familias import cargar_familias
from automatizacion_looker_aps.intervenciones.filtrado_actividades import filtro_actividades
from automatizacion_looker_aps.novedades.novedades import cargar_novedades
from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from cargar_usuarios import cargar_responsable
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025
from datetime import datetime
import gspread


def extraer_distribucion_redes():
    url_distribucion_redes = "https://docs.google.com/spreadsheets/d/1qChAneFYqnsUHOMPrxgPLNY36vqn-bmtMq6qnVTrYI4/export?format=csv&gid=0"
    return pd.read_csv(url_distribucion_redes)


def main():

    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False  # Disable autocommit
    )

    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    FE_REPORTE = datetime.now().strftime('%Y-%m-%d')



    creds = Credentials.from_service_account_file("credentials.json", scopes=scope)

    client = gspread.authorize(creds)

    os.makedirs(F'../reportes/{FE_REPORTE}/looker', exist_ok=True)

    df_distribucion_redes = extraer_distribucion_redes()


    try:

        cursor = connection.cursor()
        personas = cargar_personas(cursor, df_distribucion_redes,FE_REPORTE,client,DATABASE,"1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
        connection.commit()

        #personas_url = "F:/APS AUTOMATIZACIONES/reportes/2026-03-03/looker/cosolidado_personas_2026-03-03.csv"
        #df_personas = pd.read_csv(personas_url)

        cursor = connection.cursor()
        familias = cargar_familias( cursor, df_distribucion_redes,FE_REPORTE,client,DATABASE,"1HbJo2ZINdgZshcAIj7I1u-azaXPZHd1KbNdsdTASQGI")
        connection.commit()

        #familias_url = "F:/APS AUTOMATIZACIONES/reportes/2026-03-03/looker/cosolidado_personas_2026-03-03.csv"
        #df_familia = pd.read_csv(familias_url)

        cursor = connection.cursor()
        responsables_ebs = cargar_responsables( cursor, DATABASE)
        connection.commit()

        cursor = connection.cursor()
        filtro_actividades(cursor, familias, DATABASE, personas,FE_REPORTE,client,"14NIa4AlbXU5pVXmhLBbJwnidy4HmI2ZatFfMJ91ThXw",responsables_ebs)
        connection.commit()

        cursor = connection.cursor()
        cargar_novedades(cursor, df_distribucion_redes,FE_REPORTE,client,DATABASE,"1Yr9gvmWQ7i6ANgI-9LAW8Yfwi6HScJfF7ll3nm0StTE")
        connection.commit()


    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()