from base64 import decode

import mysql
import pandas as pd
import mysql.connector
from credenciales import MYSQL_APS, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, DATABASE_APS2024
from mysql_conector import ejecutar_consulta_mysql


def main():
    ruta = "cv/APS124CCFP20251222NI000900091143.txt"

    # Leer líneas
    with open(ruta, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    nuevas_lineas = []
    contador = 1

    for linea in lineas:
        partes = linea.strip().split("|")

        # Solo procesamos si hay suficientes columnas
        if len(partes) >= 2:
            partes[1] = str(contador)  # MODIFICAR columna 2
            contador += 1

        nuevas_lineas.append("|".join(partes) + "\n")

    # Guardar en el mismo archivo
    with open(ruta, "w", encoding="utf-8") as f:
        f.writelines(nuevas_lineas)

    print("Columna 2 actualizada correctamente.")


def cargar_indicadores():
    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2024,
        autocommit=False  # Disable autocommit
    )

    url_ccv ="cv/datos_cargar.xlsx"

    df_indicadores = pd.read_excel(url_ccv)

    try:
        cursor = connection.cursor()
        sql = "INSERT INTO agsolutic_alpha_2025.parametros (resultado, indicador, curso) VALUES (%s, %s, %s)"
        records = df_indicadores[['resultados', 'indicadores','cursos']].fillna('').astype(str).values.tolist()
        cursor.executemany(sql, records)

        connection.commit()
    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()

