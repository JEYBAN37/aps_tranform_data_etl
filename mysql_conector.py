# Databricks notebook source
def ejecutar_consulta_mysql(query,cursor,params=None):
    """Execute a query using the provided cursor and return the fetched rows as a list of tuples.
    The previous implementation converted rows to strings and used a set which removed duplicates and
    lost ordering, causing mismatches when assigning DataFrame columns. This version preserves row
    order and original types.
    """
    cursor.execute(query, params)
    data = cursor.fetchall()
    # Return rows as a list of tuples (preserve order and types)
    return [tuple(row) for row in data]
