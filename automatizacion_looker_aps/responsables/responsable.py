import pandas as pd

from automatizacion_looker_aps.familias.comunes import extraer_por_bases
from automatizacion_looker_aps.query.query_responsable import query_responsables
from mysql_conector import ejecutar_consulta_mysql


def extraer_responsables(cursor, bases: list[str]) -> pd.DataFrame:
    return extraer_por_bases(cursor, bases, query_responsables, "responsables")