import json

import pandas as pd

from automatizacion_looker_aps.intervenciones.filtrado_actividades import json_to_dict
from automatizacion_looker_aps.utils.cargar_big_query import cargar_csv_a_bigquery
from automatizacion_looker_aps.utils.sobrescribir_sheets import sobrescribir_hoja


# 1. Función de conversión segura y robusta
def forzar_diccionario(dato):
    if pd.isna(dato):
        return {}

    # Si por algún proceso previo ya es una lista, tomamos el primer elemento
    if isinstance(dato, list) and len(dato) > 0:
        dato = dato[0]

    if isinstance(dato, dict):
        return dato

    try:
        # 1. Intentar cargar el JSON string
        resultado = json.loads(str(dato))

        # 2. Si el resultado es una lista (como tu estructura con [ ... ]), sacamos el dict
        if isinstance(resultado, list) and len(resultado) > 0:
            resultado = resultado[0]

        # 3. Si por doble serialización sigue siendo un string, cargamos otra vez
        if isinstance(resultado, str):
            resultado = json.loads(resultado)
            if isinstance(resultado, list) and len(resultado) > 0:
                resultado = resultado[0]

        return resultado if isinstance(resultado, dict) else {}

    except (json.JSONDecodeError, TypeError):
        return {}


def agregar_filas_intervenciones_validas(row, df_interveciones_validas):
    # Usamos tu función para obtener la lista/diccionario del JSON
    registro_json = json_to_dict(row, 'actividaddesarrollar')
    print(row['familia_id'])

    # Si tu función devuelve un dict único en lugar de una lista, lo metemos en una lista para el for
    if isinstance(registro_json, dict):
        registro_json = [registro_json]

    for situacion_priorizada in registro_json:
        print("Procesando fila con id_familia_plan:", situacion_priorizada)

        # Corregido: .loc[len(...)] inserta los datos en el DataFrame acumulador externo
        df_interveciones_validas.loc[len(df_interveciones_validas)] = {
            'id_familia_plan': row['familia_id'],
            'id_plan_cuidado': row['observacion_id'] if 'observacion_id' in row else '',
            # Corregido: Uso de .get() por si acaso o corchetes para Series de Pandas
            'id_sociambiental': row['sociambiental_id'] if 'sociambiental_id' in row else '',
            'situacion_priorizada': situacion_priorizada.get('situacionesPriorizadas', ''),
            # Cambiado a la llave del JSON
            'estado': situacion_priorizada.get('estado', ''),
            'fecha_compromiso': situacion_priorizada.get('fechaCompromiso', ''),
            'fecha_seguimiento': situacion_priorizada.get('fechaSeguimiento', ''),
            'objetivo': str(situacion_priorizada.get('objetivoCortoPlazo', '')),
            'resultado': str(situacion_priorizada.get('resultadosEsperados', '')),
            # Corregido: 'responsableFamilia' viene de la variable interna del JSON, no de la fila
            'id_persona': situacion_priorizada.get('responsableFamilia', ''),
        }


def verificar_indicadores(df_intervenciones,sheet_id,client):
    # Descartar filas con NaN en la columna crítica
    df_validas = df_intervenciones.dropna(subset=['actividaddesarrollar']).copy()

    # Estructura del DataFrame donde se van a ir guardando/acumulando los registros desglosados
    df_interveciones_validas = pd.DataFrame(columns=[
        'id_familia_plan', 'id_sociambiental', 'situacion_priorizada',
        'estado', 'fecha_compromiso', 'fecha_seguimiento', 'objetivo',
        'resultado', 'id_persona','id_plan_cuidado'
    ])

    # Se ejecuta el apply pasando el DataFrame acumulador como argumento extra
    df_validas.apply(
        lambda row: agregar_filas_intervenciones_validas(row, df_interveciones_validas),
        axis=1
    )

    df_interveciones_validas['resultado'] = df_interveciones_validas['resultado'].apply(
        lambda x: x.replace(',', '$') if isinstance(x, str) else x
    ).fillna(0)

    df_interveciones_validas['resultado'] = df_interveciones_validas['resultado'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('"','').strip() if isinstance(x, str) else x
    ).fillna(0)

    df_interveciones_validas['objetivo'] = df_interveciones_validas['objetivo'].apply(
        lambda x: x.replace(',', '$') if isinstance(x, str) else x
    ).fillna(0)

    df_interveciones_validas['objetivo'] = df_interveciones_validas['objetivo'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('"', '').strip() if isinstance(x, str) else x
    ).fillna(0)

    # Retornamos el DataFrame acumulador que ya se llenó con el proceso de arriba
    cargar_csv_a_bigquery(df_interveciones_validas, table_id="datos_aps.situaciones_priorizadas", project_id="aps-project-478903",
                          )

    sobrescribir_hoja(sheet_id, "ST", df_interveciones_validas,client)


