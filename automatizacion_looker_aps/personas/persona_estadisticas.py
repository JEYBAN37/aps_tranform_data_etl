import pandas as pd

from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025
import mysql.connector

from limpieza_datos import extraer_distribucion_redes

if __name__ == "__main__":
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
        responsables_ebs = cargar_responsables(cursor, DATABASE)
        connection.commit()

        cursor = connection.cursor()

        personas = cargar_personas(cursor, extraer_distribucion_redes, '', 'client', DATABASE,
                                   "1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
        connection.commit()




    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()