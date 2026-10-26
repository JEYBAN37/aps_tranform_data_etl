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
    registro_json = json_to_dict(row, 'actividaddesarrollar')
    print(row['familia_id'])

    if isinstance(registro_json, dict):
        registro_json = [registro_json]

    for situacion_priorizada in registro_json:
        print("Procesando fila con id_familia_plan:", situacion_priorizada)

        id_responsable = situacion_priorizada.get('responsableEBS', '')

        print("Situación priorizada:", situacion_priorizada)

        # Se inserta la fila asegurando que todas las llaves existan en las columnas del DataFrame
        df_interveciones_validas.loc[len(df_interveciones_validas)] = {
            'id_familia_plan': row['familia_id'],
            'id_plan_cuidado': row.get('observacion_id', ''),
            'id_sociambiental': row.get('sociambiental_id', ''),
            'situacion_priorizada': situacion_priorizada.get('situacionesPriorizadas', ''),
            'estado': situacion_priorizada.get('estado', ''),
            'fecha_compromiso': situacion_priorizada.get('fechaCompromiso', ''),
            'fecha_seguimiento': situacion_priorizada.get('fechaSeguimiento', ''),
            'objetivo': str(situacion_priorizada.get('objetivoCortoPlazo', '')),
            'resultado': str(situacion_priorizada.get('resultadosEsperados', '')),
            'id_persona': situacion_priorizada.get('responsableFamilia', ''),
            'id_responsable': id_responsable  # <--- Esta columna faltaba en el esquema inicial
        }


def verificar_indicadores(df_intervenciones, sheet_id, client, df_responsables):
    df_validas = df_intervenciones.dropna(subset=['actividaddesarrollar']).copy()

    # 1. CORRECCIÓN: Definir TODAS las columnas del DataFrame objetivo (incluyendo id_responsable)
    df_interveciones_validas = pd.DataFrame(columns=[
        'id_familia_plan', 'id_plan_cuidado', 'id_sociambiental',
        'situacion_priorizada', 'estado', 'fecha_compromiso',
        'fecha_seguimiento', 'objetivo', 'resultado',
        'id_persona', 'id_responsable'  # <--- Agregada aquí
    ])

    # Aplicar la función para poblar el DataFrame
    df_validas.apply(
        lambda row: agregar_filas_intervenciones_validas(row, df_interveciones_validas),
        axis=1
    )

    # Limpieza de textos en columnas 'resultado' y 'objetivo'
    for col in ['resultado', 'objetivo']:
        df_interveciones_validas[col] = (
            df_interveciones_validas[col]
            .astype(str)
            .str.replace(',', '$', regex=False)
            .str.replace(r'[\[\]"]', '', regex=True)
            .str.strip()
        )

    # 1. Asegurar que las llaves tengan el mismo tipo de dato numérico
    # errors='coerce' convierte valores inválidos/vacíos a NaN en lugar de romper el código
    df_interveciones_validas['id_responsable'] = pd.to_numeric(
        df_interveciones_validas['id_responsable'], errors='coerce'
    ).astype('Int64')  # 'Int64' con I mayúscula permite manejar nulos de forma segura

    df_responsables['id'] = pd.to_numeric(
        df_responsables['id'], errors='coerce'
    ).astype('Int64')

    # 2. Ahora el merge funcionará sin errores de tipo
    df_interveciones_validas = df_interveciones_validas.merge(
        df_responsables[['id', 'nombres']],
        left_on='id_responsable',
        right_on='id',
        how='left'
    )

    # Retornamos el DataFrame acumulador que ya se llenó con el proceso de arriba
    cargar_csv_a_bigquery(df_interveciones_validas, table_id="datos_aps.situaciones_priorizadas", project_id="aps-project-478903",
                          )

    sobrescribir_hoja(sheet_id, "ST", df_interveciones_validas,client)


