import logging
import re  # Correctly import the re module

import jaydebeapi
import pandas as pd

from credenciales import DRIVER_MYSQL, MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DRIVER_PATH

def extraer_barrio(texto, barrios=None):
    if pd.isna(texto):
        return None
    texto_upper = texto.upper()  # Convert the string to uppercase
    for barrio in barrios:
        if re.search(r'\b' + re.escape(barrio) + r'\b', texto_upper):
            return barrio
    # If no match is found, return the original text stripped of whitespace
    return texto.strip()


def main():
    conexion_replica = jaydebeapi.connect(
        DRIVER_MYSQL,
        MYSQL_APS,
        [MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD],
        DRIVER_PATH
    )

    connection_mysql_replica = conexion_replica.cursor()
    try:
        # Cargar CV https://docs.google.com/spreadsheets/d/1RqbfsgJc9N-qOJfmveLQTm1GNwwbO-0S/export?format=csv&gid=749204967
        df = pd.read_csv('cv/barrios.csv', encoding='latin-1')
        df_ubicaciones = pd.read_csv('cv/nombres.csv', encoding='latin-1')
        # Lista de barrios conocidos (puedes ampliarla)
        barrios = [
            "OBRERO", "CANCHALA", "BUESAQUILLO", "SANTA MONICA", "SANTA BARBARA",
            "POPULAR", "GUAMUEZ", "SIETE DE AGOSTO", "VILLA RECREO", "VILLAS DEL VIENTO",
            "LAS MERCEDES", "CAROLINA", "SANTA FE", "MIRAFLORES", "STA MARIA",
            "PUCALPA", "JULIAN BUCHELI"
        ]

        # Aplicar la función para extraer el barrio
        df['barrio_limpio'] = df['Barrios'].apply(lambda x: extraer_barrio(x, df_ubicaciones['BARRIO'].str.upper()))
        # Mostrar resultados
        print(df)

    except Exception as e:
        logging.error(f"{e}")
        return
    finally:
        connection_mysql_replica.close()
        conexion_replica.close()

if __name__ == "__main__":
    main()