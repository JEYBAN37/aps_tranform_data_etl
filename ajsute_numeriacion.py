from base64 import decode
import random

import mysql
import pandas as pd
import mysql.connector
from rdflib.tools.csv2rdf import column

from credenciales import MYSQL_APS, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, DATABASE_APS2024
from export_aps_124 import limpiar_tildes
from mysql_conector import ejecutar_consulta_mysql

#1|NI|900145767|2026-06-30|2026-06-30|947
def main():
    ruta = r"F:\APS AUTOMATIZACIONES\aps\reportes\SER124DREC20260630NI000900091143ID2087325712.txt"

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


import random


def nombrar_responsables_archivo_aps():
    ruta_archivo_plano = "C:/Users/Esteban Getial/OneDrive/Desktop/F.txt"
    ruta_salida = "C:/Users/Esteban Getial/OneDrive/Desktop/CORREJIDO.txt"

    # Lista originalizada con [Nombre, Rol, Cédula]
    responsables_origen = [
        ["Edith Angelica López Villarreal", "TERAPEUTAOCUPACIONAL", 59310145],
        ["YURANI NATALI BURBANO DIAZ", "PSICOLOGO", 1086360022],
        ["Juan Pablo Madroñero Muñoz", "PSICOLOGO", 13069636],
        ["Carol Silvana Patiño", "PSICOLOGO", 1085313870],
        ["Cindy Estefanie Gonzalez Del Castillo", "PSICOLOGO", 1085303067],
        ["DANNY ALEXANDER SANCHEZ ESTRADA", "PSICOLOGO", 1085264667],
        ["YOHANA KATERING PANTOJA MELO", "MEDICO", 1089846684],
        ["Jeisson Steven Basante Erazo", "GESTORCOMUNITARIO", 1085347173],
        ["Lady Carolina Tibaquirá Ballesteros", "ENFERMERA", 1014212900],
        ["CARMEN LILIANA ARMERO RUIZ", "ENFERMERA", 1085248939],
        ["JOSE EDILBERTO CORDOBA PANTOJA", "ENFERMERA", 1089844032],
        ["VANESSA KATHERINE JOJOA ARIAS", "AUXILIARESDEENFERMERIA", 1085286742],
        ["Pablo José Bados López", "AUXILIARESDEENFERMERIA", 1004232508],
        ["GABRIEL SEBASTIAN MORINELLY ROJAS", "AUXILIARESDEENFERMERIA", 1080046034],
        ["María José Leitón Zutta", "AUXILIARESDEENFERMERIA", 1085327701],
        ["YONATAN MENA GRIJALBA", "AUXILIARESDEENFERMERIA", 87030128],
        ["Andrea Nathaly Alava Bolaños", "AUXILIARESDEENFERMERIA", 1004216993],
        ["KAROL DAYANA TONGUINO TORO", "AUXILIARESDEENFERMERIA", 1137624011],
        ["LORENA DEL CARMEN OJEDA VASQUEZ", "AUXILIARESDEENFERMERIA", 1086329609],
        ["MARIA ALEJANDRA DIAZ ORTEGA", "AUXILIARESDEENFERMERIA", 1086361622],
        ["KELLY YURANY JOJOA BOTINA", "AUXILIARESDEENFERMERIA", 1085339190],
        ["TANIA KARINA CUERO CASANOVA", "AUXILIARESDEENFERMERIA", 1004235480],
        ["Yolldy Lizeth Cabrera", "AUXILIARESDEENFERMERIA", 1088733625],
        ["KARELIS NATALY ZHENG MONSALVE", "AUXILIARESDEENFERMERIA", 5930737],
        ["ZULY MARIANA HIDALGO JOJOA", "AUXILIARESDEENFERMERIA", 1086328142],
    ]


    # Agrupamos las CÉDULAS por ROL en el diccionario
    roles_dict = {}
    for nombre, rol, cedula in responsables_origen:
        rol_key = rol.upper().strip()
        if rol_key not in roles_dict:
            roles_dict[rol_key] = []
        # Guardamos la cédula directamente como texto para evitar problemas de formateo posterior
        roles_dict[rol_key].append(str(cedula))

    # Leemos las líneas del archivo plano
    with open(ruta_archivo_plano, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    lineas_procesadas = []

    # Iteramos línea por línea del archivo plano
    for linea in lineas:
        partes = linea.strip().split("|")

        # Verificamos que la línea tenga las columnas necesarias (mínimo índice 23)
        if len(partes) > 23:
            rol_en_linea = partes[23].strip().upper()
            nombre_actual_en_linea = partes[22].strip()

            # Solo asignamos si el campo de la cédula/responsable (columna 22) está vacío

                # Si el rol de la línea coincide con nuestro diccionario
            if rol_en_linea in roles_dict:
                    # Selecciona una cédula aleatoria de la lista de ese rol
                    cedula_aleatoria = random.choice(roles_dict[rol_en_linea])

                    # Como es un número de cédula convertido a texto, removemos espacios por seguridad
                    # (Ya no usamos .upper() porque las cédulas no llevan letras)
                    partes[22] = cedula_aleatoria.replace(" ", "")

        # Volvemos a armar la línea manteniendo la estructura original
        lineas_procesadas.append("|".join(partes) + "\n")

    # Guardamos los resultados en el archivo de salida
    with open(ruta_salida, "w", encoding="utf-8") as f:
        f.writelines(lineas_procesadas)

    print("¡Archivo de APS procesado con éxito! Se asignaron las CÉDULAS de forma aleatoria.")

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

def sumar_pagos():
    ruta = "E:\PAGOS_EBS_PROGRAMA_OFICIAL (1).xlsx"
    df_contratos = pd.read_excel(ruta, sheet_name="CONTRATOS")
    df_pagos = pd.read_excel(ruta, sheet_name="REPORTADOS")

    df_pagos_grouped = df_pagos.groupby('numero_contrato')['valor'].sum().reset_index()

    df_contratos['valor_pagado'] = df_contratos['numero_contrato'].map(df_pagos_grouped.set_index('numero_contrato')['valor'])

    df_contratos.to_excel("E:\PAGOS_EBS_PROGRAMA_OFICIAL_SUMADO.xlsx", index=False)
    print(df_contratos[['numero_contrato', 'valor_pagado']])

if __name__ == "__main__":
    #sumar_pagos()
    main()
    #nombrar_responsables_archivo_aps()
    #aps_cedular_ya_reportadas()
    #cargar_indicadores()
    #unir_csv_falla_coordenadas()
    #unir_csv_falla_familias()
    #convertir_to_json()

