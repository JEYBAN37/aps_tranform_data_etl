import pandas as pd

from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025
import mysql.connector

from limpieza_datos import extraer_distribucion_redes


def procesar_base():

    url ="bases_downloads/Informe de Produccion Servicio Plan Unidad (13).xlsx"
    produccion = pd.read_excel(url, skiprows=2)
    # Luego eliminamos las primeras 2 columnas si no las necesitas
    df_produccion = produccion.iloc[:, 2:]
    print(df_produccion.head())

    df_responsables,df_personas = df_cruce_con_db()

    df_responsables_activo = df_responsables[df_responsables['contrato'] == 'ACTIVO']

    df_produccion_filtrada = df_produccion[df_produccion['Ident Medico'].isin(df_responsables_activo['numero'])]

    df_produccion_filtrada.drop_duplicates(subset=['Identificacion'], inplace=True)

    df_hogares_atendidos = df_produccion_filtrada[df_produccion_filtrada['Identificacion'].isin(df_personas['numero'])]

    print(f"Hay {len(df_hogares_atendidos)} filas")
    print(f"Hay {len(df_produccion_filtrada)} filas")

def df_cruce_con_db():
    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False  # Disable autocommit
    )

    try:

        cursor = connection.cursor()
        responsables_ebs = cargar_responsables( cursor, DATABASE)
        connection.commit()


        cursor = connection.cursor()
        df_distribucion_redes = extraer_distribucion_redes()
        personas = cargar_personas(cursor, df_distribucion_redes,'','client',DATABASE,"1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
        connection.commit()

        return responsables_ebs,personas



    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    procesar_base()