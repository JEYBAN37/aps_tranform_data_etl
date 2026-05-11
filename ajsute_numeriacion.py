from base64 import decode

import mysql
import pandas as pd
import mysql.connector
from rdflib.tools.csv2rdf import column

from credenciales import MYSQL_APS, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, DATABASE_APS2024
from mysql_conector import ejecutar_consulta_mysql

# 1|NI|900091143|2026-03-01|2026-03-31|260
def main():
    ruta = "reportes/SER124DREC20260331NI000900091143ID2087325712.txt"

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


def aps_cedular_ya_reportadas():
    ruta = "E:\APS124CCFP20251208NI000900091143.txt"

    # Leer líneas
    with open(ruta, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    nuevas_lineas = []
    cedulas = []
    for linea in lineas:
        partes = linea.strip().split("|")

        # Solo procesamos si hay suficientes columnas (índice 7 -> se necesitan al menos 8 columnas)
        if len(partes) >= 8:
            cedulas.append(partes[7])
        else:
            cedulas.append(None)

    df_cedulas = pd.DataFrame({'cedula': cedulas})


    # Guardar en el mismo archivo
    df_cedulas.to_csv("reportes/cedulas_reportadas.csv", index=False)

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





def unir_csv_falla_coordenadas ():
    ruta = "reportes/2025-12-05/fallas_coordenadas"
    df_consolidado = pd.DataFrame()
    for i in range(1,19):
        ruta_csv = f"{ruta}/{i}.csv"
        df = pd.read_csv(ruta_csv)
        df_consolidado = pd.concat([df_consolidado, df], ignore_index=True)


    ruta_salida = f"{ruta}/fallas_coordenadas_consolidado.csv"
    df_consolidado.to_csv(ruta_salida, index=False)
    print("Archivos CSV unidos correctamente.")

def unir_csv_falla_familias ():
    ruta = "reportes/2025-12-05/fallas_familias"
    df_consolidado = pd.DataFrame()
    for i in range(1,5):
        ruta_csv = f"{ruta}/{i}.csv"
        df = pd.read_csv(ruta_csv)
        df_consolidado = pd.concat([df_consolidado, df], ignore_index=True)


    ruta_salida = f"{ruta}/fallas_familia_consolidado.csv"
    df_consolidado.to_csv(ruta_salida, index=False)
    print("Archivos CSV unidos correctamente.")

def convertir_to_json():
    sheet_id = "1dqkisXc5OQNKTWd4YgMKnW5vu382FSR6"
    sheet_name = "CRONOGRAMA_SEMANA_4"  # El nombre de la pestaña

    # Formateamos la URL para descargar como CSV
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    df = pd.read_csv(url)
    df['id'] = df.index + 1

    # Convert common date columns to a JS-friendly array: [year, monthIndex, day, hour, minute]
    date_columns = ['fechaInicio', 'fechaFin', 'fecha_inicio', 'fecha_fin', 'start', 'end']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            def to_js_array(dt):
                if pd.isna(dt):
                    return None
                return [int(dt.year), int(dt.month), int(dt.day), int(dt.hour), int(dt.minute)]
            df[col] = df[col].apply(to_js_array)

    df['territorio'] = df['territorio'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['celular'] = df['celular'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))

    # If `url` column exists, ensure missing values become JSON null
    if 'url' in df.columns:
        df['url'] = df['url'].apply(lambda x: None if pd.isna(x) else x)

    # Replace any remaining NaN with None so json.dump writes null
    df = df.where(pd.notnull(df), None)

    # Write records with native Python structures so JSON arrays/nulls are preserved
    import json
    ruta_json = "cv/Formato_Cronograma.json"
    records = df.to_dict(orient='records')
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)



if __name__ == "__main__":
    #main()
    aps_cedular_ya_reportadas()
    #cargar_indicadores()
    #unir_csv_falla_coordenadas()
    #unir_csv_falla_familias()
    #convertir_to_json()

