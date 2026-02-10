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
        url = 'cv/2026.xlsx'
        df = pd.read_excel(url, engine='openpyxl',   dtype=str)  # Ensure `openpyxl` is installed

        df_usuarios_nuevos = df[
            df['N° CEDULA'].notnull() & (df['N° CEDULA'] != '')
            ].copy()

        df_usuarios_nuevos['username'] = df_usuarios_nuevos['N° CEDULA'].astype(str).str.split('.').str[0]
        df_usuarios_nuevos['nombre'] = df_usuarios_nuevos['NOMBRE'].astype(str).str.upper()
        df_usuarios_nuevos['celular'] = df_usuarios_nuevos['TELEFONO'].astype(str).str.upper()
        df_usuarios_nuevos['perfil'] = df_usuarios_nuevos['PERFIL'].astype(str).str.upper()
        df_usuarios_nuevos['red'] = df_usuarios_nuevos['RED'].astype(str).str.upper()
        df_usuarios_nuevos['ebs'] = df_usuarios_nuevos.apply(
            lambda row: f"{str(row['EBS 1']).upper()} {str(row['EBS 2']).upper()}" if str(
                row['EBS 2']).upper() != '0' else str(row['EBS 1']).upper(),
            axis=1
        )

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

            #suspender_responsables(connection_mysql_replica=cursor,
                        #df_usuarios_nuevos=df_usuarios_nuevos,
                        #database=databse)

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

        # Preparar el DataFrame para la carga masiva
        df_actualizar = df_usuarios_nuevos[df_usuarios_nuevos['username'].isin(df_antiguos['username'])].copy()


        df_borrar = df_antiguos[~df_antiguos['username'].isin(df_actualizar['username'])].copy()


        df_usuarios_consolidado = pd.DataFrame(index=df_usuarios_para_preparar.index)
        df_usuarios_consolidado['nivel'] = 'D'
        df_usuarios_consolidado['group_id'] = 3
        df_usuarios_consolidado['password'] = df_usuarios_para_preparar['password']
        df_usuarios_consolidado['username'] = df_usuarios_para_preparar['username']
        df_usuarios_consolidado['numero'] = df_usuarios_para_preparar['username']
        df_usuarios_consolidado['nombre'] = df_usuarios_para_preparar['nombre']
        df_usuarios_consolidado['celular'] = df_usuarios_para_preparar['celular']
        df_usuarios_consolidado['nodo'] = df_usuarios_para_preparar['red']
        df_usuarios_consolidado['profesion'] = df_usuarios_para_preparar['perfil']

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
            print("No hay usuarios nuevos para agregar.")


        if not df_actualizar.empty:
            for _, row in df_actualizar.iterrows():
                query_update = f"""
                        UPDATE {database}.users
                        SET nombre = '{row['nombre']}', password = '{row['password']}'
                        WHERE username = '{row['username']}';
                        """
                connection_mysql_replica.execute(query_update)
            print(f"Se actualizaron {len(df_actualizar)} usuarios.")
        else:
            print("No hay usuarios para actualizar.")

        if not df_borrar.empty:
            for _, row in df_borrar.iterrows():
                query_delete = f"""
                DELETE FROM {database}.users
                WHERE username = '{row['username']}';
                """
                connection_mysql_replica.execute(query_delete)
            print(f"Se eliminaron {len(df_borrar)} usuarios.")
        else:
            print("No hay usuarios para eliminar.")


def cargar_responsable (connection_mysql_replica=None, conexion_replica=None, df_usuarios_nuevos = None,database=None):

    datos_replica = ejecutar_consulta_mysql(f'SELECT * FROM {database}.responsables u ',
                                            connection_mysql_replica)
    df_antiguos = pd.DataFrame(datos_replica,
                               columns=[desc[0].lower() for desc in connection_mysql_replica.description],  dtype=str)

    df_antiguos.loc['nombres'] = df_antiguos['nombres'].astype(str).str.upper()


    df_usuarios_para_preparar = df_usuarios_nuevos[~df_usuarios_nuevos['username'].isin(df_antiguos['numero'] )
                                                                                        & (~df_usuarios_nuevos['nombre'].isin(df_antiguos['nombres']))].copy()


    df_antiguos['nombres'] = df_antiguos['nombres'].astype(str).str.upper()

    # Preparar el DataFrame para la carga masiva
    df_actualizar_por_cedula = df_usuarios_nuevos[df_usuarios_nuevos['username'].isin(df_antiguos['numero'])].copy()


    df_suspendidos = df_antiguos[df_antiguos['numero'] == '0'].copy()

    df_actualizar_supendidos = df_usuarios_nuevos[df_usuarios_nuevos['nombre'].isin(df_suspendidos['nombres'])].copy()


    # Initialize df_responsable_consolidado with the same index as df_usuarios_para_preparar
    df_responsable_consolidado = pd.DataFrame(index=df_usuarios_para_preparar.index)

    # Assign values to the columns
    df_responsable_consolidado.loc[:, 'tipodoc'] = 'CC'
    df_responsable_consolidado.loc[:, 'numero'] = df_usuarios_para_preparar['username']
    df_responsable_consolidado.loc[:, 'nombres'] = df_usuarios_para_preparar['nombre']
    df_responsable_consolidado.loc[:, 'celular'] = \
    df_usuarios_nuevos['celular'].fillna('SIN CELULAR').astype(str).str.split('.').str[0]
    df_responsable_consolidado.loc[:, 'nodo'] = df_usuarios_nuevos['red'].fillna('SIN NODO')
    df_responsable_consolidado.loc[:, 'ebs'] = df_usuarios_nuevos['ebs'].fillna('SIN EBS')
    df_responsable_consolidado.loc[:, 'profesion'] = df_usuarios_nuevos['PERFIL'].fillna('SIN PROFESION').astype(
            str).str.upper()


    if not df_responsable_consolidado.empty:
        # Crear una lista de valores para insertar
        values = ", ".join(
            f"('{row['tipodoc']}', '{row['numero']}', '{row['nombres']}', '{row['celular']}', '{row['profesion']}', '{row['nodo']}', '{row['ebs']}')"
            for _, row in df_responsable_consolidado.iterrows()
        )


        # Crear la consulta de inserción masiva
        query_insert = f"INSERT INTO {database}.responsables (tipodoc, numero, nombres, celular, profesion, nodo, ebs) VALUES {values}"
        connection_mysql_replica.execute(query_insert)
        print(f"Se insertaron {len(df_responsable_consolidado)} usuarios.")

    if not df_actualizar_por_cedula.empty:
        for _, row in df_actualizar_por_cedula.iterrows():
            query_update = f"""
                    UPDATE {database}.responsables
                    SET nombres = '{row['nombre']}', numero = '{row['username']}', celular = '{row['celular']}',
                     profesion = '{row['perfil']}', nodo = '{row['red']}',
                      contrato = 'ACTIVO' ,ebs = '{row['ebs']}'
                    WHERE numero = '{row['username']}';
                    """
            connection_mysql_replica.execute(query_update)
        print(f"Se actualizaron {len(df_actualizar_por_cedula)} usuarios.")
    else:
        print("No hay usuarios para actualizar por cedula.")

    if not df_actualizar_supendidos.empty:
        for _, row in df_actualizar_supendidos.iterrows():
            query_update = f"""
                     UPDATE {database}.responsables
                     SET nombres = '{row['nombre']}', numero = '{row['username']}', celular = '{row['celular']}', profesion = '{row['perfil']}', nodo = '{row['red']}', ebs = '{row['ebs']}'
                     WHERE numero = 0 AND nombres = '{row['nombre']}';
                     """
            connection_mysql_replica.execute(query_update)
        print(f"Se actualizaron {len(df_actualizar_supendidos)} usuarios.")
    else:
        print("No hay usuarios para actualizar por nombre.")


def suspender_responsables(connection_mysql_replica=None, conexion_replica=None, df_usuarios_nuevos = None,database=None):

    datos_replica = ejecutar_consulta_mysql(f'SELECT * FROM {database}.responsables u ',
                                            connection_mysql_replica)
    df_antiguos = pd.DataFrame(datos_replica,
                               columns=[desc[0].lower() for desc in connection_mysql_replica.description], dtype=str)

    df_antiguos.loc['nombres'] = df_antiguos['nombres'].astype(str).str.upper()

    df_suspender = df_antiguos[~df_antiguos['nombres'].isin(df_usuarios_nuevos['nombre'])].copy()

    if not df_suspender.empty:

        df_suspender['perfil'] = 'SUSPENDIDO'
        df_suspender['red'] = 'SUSPENDIDO'
        df_suspender['ebs'] = 'SUSPENDIDO'
        df_suspender['contrato'] = 'SUSPENDIDO'
        df_suspender['correo'] = 'SUSPENDIDO'

        for _, row in df_suspender.iterrows():
            query_delete = f"""
            UPDATE {database}.responsables
                    SET contrato = '{row['contrato']}', profesion = '{row['perfil']}', nodo = '{row['red']}', ebs = '{row['ebs']}', correo = '{row['correo']}'
                    WHERE nombres = '{row['nombres']}';
            """
            connection_mysql_replica.execute(query_delete)
        print(f"Se SUSPENDIERON  {len(df_suspender)} responsables.")
    else:
        print("No hay usuarios para suspender.")




if __name__ == "__main__":
    main()
