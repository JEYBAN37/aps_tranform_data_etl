import pandas as pd

from automatizacion_looker_aps.query.query_responsable import query_responsables
from mysql_conector import ejecutar_consulta_mysql


def cargar_responsables(cursor,db):
    acumulado_responsable = []

    for database in db:
        familias_query = ejecutar_consulta_mysql(query_responsables(database), cursor)
        if not familias_query:
            print(f"No se encontraron personas para {database}. Continuando.")
            continue
        acumulado_responsable.extend(familias_query)

    # assign accumulated results back to `familias_query` so later code can build the DataFrame
    familias = acumulado_responsable

    df_responsables = pd.DataFrame(familias)

    df_responsables.columns = [desc[0] for desc in cursor.description]

    return df_responsables