import os
import mysql.connector
import pandas as pd
from google.auth.exceptions import GoogleAuthError
from google.oauth2.service_account import Credentials
from automatizacion_looker_aps.familias.familias import cargar_familias
from automatizacion_looker_aps.intervenciones.filtrado_actividades import filtro_actividades
from automatizacion_looker_aps.intervenciones.verificar_intervenciones import verificar_indicadores
from automatizacion_looker_aps.novedades.novedades import cargar_novedades
from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
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

    path_creds = "credentials.json"
    try:
        # 1. Verificar si el archivo existe físicamente
        if not os.path.exists(path_creds):
            print(f"❌ ERROR: El archivo '{path_creds}' no existe en: {os.getcwd()}")
        else:
            print(f"✅ Archivo '{path_creds}' encontrado.")

        # 2. Intentar cargar las credenciales
        creds = Credentials.from_service_account_file(path_creds, scopes=scope)
        print("✅ Credenciales cargadas correctamente desde el archivo.")

        # 3. Probar autorización con Google Sheets
        client = gspread.authorize(creds)
        # Intentar listar archivos (solo para probar conexión)
        client.list_spreadsheet_files()
        print("🚀 ¡Conexión exitosa! No hay bloqueos de Google.")

    except GoogleAuthError as e:
        print(f"❌ Error de Autenticación: {e}")
    except Exception as e:
        print(f"❌ Se produjo un error inesperado: {e}")


    try:

        cursor = connection.cursor()
        # df_personas = cargar_personas(cursor, df_distribucion_redes,FE_REPORTE,client,DATABASE,"1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
        # connection.commit()

        #personas_url = "F:/APS AUTOMATIZACIONES/reportes/2026-05-25/looker/cosolidado_personas_2026-05-25.csv"
        #df_personas = pd.read_csv(personas_url)

        # cursor = connection.cursor()
        # df_familia , df_situaciones_priorizadas = cargar_familias( cursor, df_distribucion_redes,FE_REPORTE,client,DATABASE,"1HbJo2ZINdgZshcAIj7I1u-azaXPZHd1KbNdsdTASQGI")
        # connection.commit()

        df_situaciones_priorizadas = pd.read_csv(F'../reportes/{FE_REPORTE}/looker/situaciones_priorizadas_{FE_REPORTE}.csv')

        # familias_url = "F:/APS AUTOMATIZACIONES/reportes/2026-05-25/looker/cosolidado_familias_2026-05-25.csv"
        # df_familia = pd.read_csv(familias_url)

        # cursor = connection.cursor()
        # responsables_ebs = cargar_responsables( cursor, DATABASE)
        # connection.commit()


        # cursor = connection.cursor()
        # filtro_actividades(cursor, df_familia, DATABASE, df_personas,FE_REPORTE,client,"14NIa4AlbXU5pVXmhLBbJwnidy4HmI2ZatFfMJ91ThXw",responsables_ebs)
        # connection.commit()

        verificar_indicadores(df_situaciones_priorizadas)

        exit()


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