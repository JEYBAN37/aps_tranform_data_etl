# Insignias de Replica
import hashlib
import logging
import jaydebeapi
import mysql.connector
import pandas as pd


from credenciales import DRIVER_MYSQL, MYSQL_APS, MYSQL_REPLICA_PASSWORD, MYSQL_REPLICA_USER, DRIVER_PATH, DATABASE, \
    DATABASE_APS2024
from mysql_conector import ejecutar_consulta_mysql


# Databricks delete imports

# delete
def main():
    # crea la conexion a la base de datos replica
    global cursor


    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2024,
        autocommit=False  # Disable autocommit
    )

    try:
        cursor = connection.cursor()
        # Cargar CV https://docs.google.com/spreadsheets/d/1RqbfsgJc9N-qOJfmveLQTm1GNwwbO-0S/export?format=csv&gid=749204967
        url = 'https://docs.google.com/spreadsheets/d/1RqbfsgJc9N-qOJfmveLQTm1GNwwbO-0S/export?format=xlsx&gid=749204967'
        df = pd.read_excel(url, engine='openpyxl')  # Ensure `openpyxl` is installed

        df_usuarios_nuevos = df[
            df['N° CEDULA'].notnull() & (df['N° CEDULA'] != '')
            ].copy()

        df_usuarios_nuevos['username'] = df_usuarios_nuevos['N° CEDULA'].astype(str).str.split('.').str[0]
        df_usuarios_nuevos['nombre'] = df_usuarios_nuevos['NOMBRE'].astype(str).str.lower()
        df_usuarios_nuevos['password'] = df_usuarios_nuevos['username'].apply(
            lambda x: hashlib.md5(('Cc' + x).encode()).hexdigest())

        #se cargan los usuarios
        for databse in DATABASE:
            cargar_user(connection_mysql_replica=cursor,
                        df_usuarios_nuevos=df_usuarios_nuevos,
                        database=databse)

            cargar_responsable(connection_mysql_replica=cursor,
                        df_usuarios_nuevos=df_usuarios_nuevos,
                        database=databse)

        connection.commit()

    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()

def cargar_user( connection_mysql_replica=None, df_usuarios_nuevos = None, database=None):
    # se crea el df de recepcion de datos
        # se trae los usuarios para validar
        datos_replica = ejecutar_consulta_mysql(f'SELECT * FROM {database}.users u ',
                                                connection_mysql_replica)
        df_antiguos = pd.DataFrame(datos_replica,
                                   columns=[desc[0].lower() for desc in connection_mysql_replica.description])


        df_usuarios_para_preparar = df_usuarios_nuevos[~df_usuarios_nuevos['username'].isin(df_antiguos['username'])].copy()


        df_usuarios_consolidado = pd.DataFrame(
            columns=['nivel', 'group_id', 'tipodoc', 'numero', 'nombres', 'celular', 'nodo', 'profesion'])

        df_usuarios_consolidado.loc[:0, 'nivel'] = 'D'
        df_usuarios_consolidado.loc[:, 'group_id'] = 3

        df_usuarios_consolidado.loc[:, 'tipodoc'] = 'CC'
        df_usuarios_consolidado.loc[:, 'numero'] = df_usuarios_para_preparar['username']
        df_usuarios_consolidado.loc[:, 'nombres'] = df_usuarios_para_preparar['nombre']
        df_usuarios_consolidado.loc[:, 'celular'] = df_usuarios_nuevos['TELEFONO'].astype(str).str.split('.').str[0]
        df_usuarios_consolidado.loc[:, 'nodo'] = df_usuarios_nuevos['RED']
        df_usuarios_consolidado.loc[:, 'profesion'] = df_usuarios_nuevos['PERFIL'].astype(str).str.lower()

        # Cargar en la base de datos
        if not df_usuarios_consolidado.empty:
                # Crear una lista de valores para insertar
                values = ", ".join(
                    f"('{row['username']}', '{row['password']}', '{row['nivel']}', '{row['nombre']}', {row['group_id']})"
                    for _, row in df_usuarios_consolidado.iterrows()
                )
                # Crear la consulta de inserción masiva
                query_insert = f"INSERT INTO {database}.users (username, password, nivel, nombre, group_id) VALUES {values}"
                connection_mysql_replica.execute(query_insert)
                print(f"Se insertaron {len(df_usuarios_consolidado)} usuarios.")
        else:
            print("No hay usuarios nuevos para cargar.")


def cargar_responsable (connection_mysql_replica=None, conexion_replica=None, df_usuarios_nuevos = None,database=None):

    datos_replica = ejecutar_consulta_mysql(f'SELECT * FROM {database}.responsables u ',
                                            connection_mysql_replica)
    df_antiguos = pd.DataFrame(datos_replica,
                               columns=[desc[0].lower() for desc in connection_mysql_replica.description])

    df_antiguos['numero'] = df_antiguos['numero'] = df_antiguos['numero'].astype(str).str.replace(',', '').str.strip()
    df_usuarios_para_preparar = df_usuarios_nuevos[~df_usuarios_nuevos['username'].isin(df_antiguos['numero'])].copy()


    # Cargar en la base de datos
    if not df_usuarios_para_preparar.empty:
        # Initialize df_responsable_consolidado with the same index as df_usuarios_para_preparar
        df_responsable_consolidado = pd.DataFrame(index=df_usuarios_para_preparar.index)

        # Assign values to the columns
        df_responsable_consolidado.loc[:, 'tipodoc'] = 'CC'
        df_responsable_consolidado.loc[:, 'numero'] = df_usuarios_para_preparar['username']
        df_responsable_consolidado.loc[:, 'nombres'] = df_usuarios_para_preparar['nombre']
        df_responsable_consolidado.loc[:, 'celular'] = \
        df_usuarios_nuevos['TELEFONO'].fillna('SIN CELULAR').astype(str).str.split('.').str[0]
        df_responsable_consolidado.loc[:, 'nodo'] = df_usuarios_nuevos['RED'].fillna('SIN NODO')
        df_responsable_consolidado.loc[:, 'profesion'] = df_usuarios_nuevos['PERFIL'].fillna('SIN PROFESION').astype(
            str).str.lower()

        # Create a list of values for insertion
        values = ", ".join(
            f"('{row['tipodoc']}', '{row['numero']}', '{row['nombres']}', '{row['celular']}', '{row['profesion']}')"
            for _, row in df_responsable_consolidado.iterrows()
        )
        # Crear la consulta de inserción masiva
        query_insert = f"INSERT INTO {database}.responsables (tipodoc, numero, nombres, celular, profesion) VALUES {values}"
        connection_mysql_replica.execute(query_insert)
        print( f"Se insertaron {len(df_responsable_consolidado)} responsables.")
    else:
        print("No hay responsables nuevos para cargar.")

if __name__ == "__main__":
    main()
