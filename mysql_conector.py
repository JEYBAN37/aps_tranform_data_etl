# Databricks notebook source
def ejecutar_consulta_mysql(query,cursor,params=None):
    # Load the data from the database into a Spark DataFrame
    cursor.execute(query,params)
    # Obtener los datos y las columnas
    data = cursor.fetchall()
    data_processed = set()
    for row in data:
        processed_row = tuple(str(value) for value in row)  # Convertir cada valor a str y usar tuple
        data_processed.add(processed_row)

    # Crear el DataFrame con las columnas en minúsculas
    return [list(row) for row in data_processed]