from datetime import datetime
import pandas as pd

from export_aps_124 import limpiar_tildes
from export_usuarios_institucionales import codificar_formato
from propiedades_aps124 import PROPIEDADES_TIPO_2


def df_tipo_1(tipo_registro, propiedades_tipo_1, num):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': propiedades_tipo_1[0],
        'numero_identificacion': propiedades_tipo_1[1],
        'fecha_inicial': propiedades_tipo_1[2],
        'fecha_final': propiedades_tipo_1[3],
        'numero_total_registros': num
    }])

    # poner mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()

    return formato

def df_tipo_2(tipo_registro, propiedades_tipo_2, nit):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'recurso': propiedades_tipo_2[0][i],
        'nit': nit,
        'codigo': propiedades_tipo_2[1][i],
        'tipo_persona': propiedades_tipo_2[2],
        'codigo_actividad_economica': propiedades_tipo_2[3][i],
        'fecha_resolucion': propiedades_tipo_2[4][i],
        'valor_matricula': propiedades_tipo_2[5][i],
    } for i in range(len(propiedades_tipo_2[0]))])

    # poner mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()

    formato.insert(1, 'consecutivo_registro', range(1, 1 + len(formato)))

    return formato

def convertidor_objeto(row):

    row_sin_caracteres_especiales = limpiar_tildes(row)

    return row_sin_caracteres_especiales[:300]  # Limitar a 300 caracteres

def df_tipo_3(tipo_registro, inicio_consecutivo, propiedades_tipo_3, df_personas, nit):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': row['id_recurso'],
        'nit': nit,
        'indicador': row['indicador'],
        'tipo_contrato': propiedades_tipo_3[1],
        'numero_contrato': row['numero_contrato'],
        'fecha_inicio_contrato': row['fecha_inicio_contrato'].date(),
        'fecha_termino_contrato':  pd.to_datetime(row['fecha_termino_contrato']).strftime('%Y-%m-%d') if pd.notna(row['fecha_termino_contrato']) else '',
        'objeto_contrato': convertidor_objeto(row['objeto']).upper(),
        'valor': f"{row['valor_contratado']}0",
        'contratista_tipo_identificacion': row['contratista_tipo_identificacion'],
        'numero_identificacion': str(row['numero_identificacion']).split('.')[0] if pd.notna(row['numero_identificacion']) else '',
        'nombre_contratista': limpiar_tildes(row['nombre_contratista']) if pd.notna(row['nombre_contratista']) and row['nombre_contratista'] != '' else '',
        'tipo_doc_supervisor': row['tipo_doc_supervisor'],
        'numero_supervisor': str(row['numero_supervisor']).split('.')[0] if pd.notna(row['numero_supervisor']) else '',
        'nombre_supervisor': limpiar_tildes(row['nombre_supervisor']).upper() if pd.notna(row['nombre_supervisor']) else '',
    } for _, row in df_personas.iterrows()])

    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato

def df_tipo_4(tipo_registro, df ,nit, inicio_consecutivo):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': row['id_recurso'],
        'nit': nit,
        'indicador': row['indicador'],
        'codigo': row['codigo'],
        'numero_contrato': row['numero_contrato'],
        'poliza': row['poliza'],
        'fecha': row['fecha'].date(),
    } for _, row in df.iterrows()])

    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato


def obtenert_porcentaje(df, param, param1):
    total = df[df['numero_contrato'] == param]['valor'].sum()
    if total > 0:
        porcentaje = (param1 / total) * 100
        return f"{porcentaje:.1f}"
    return "0.00"


def df_tipo_5(tipo_registro,df,nit, inicio_consecutivo):
    # remove fully empty rows (all NaN) and rows where every value is empty/whitespace
    df = df.dropna(how='all').reset_index(drop=True)
    df = df[~df.apply(lambda r: r.astype(str).str.strip().eq('').all(), axis=1)].reset_index(drop=True)
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': row['id_recurso'],
        'nit': nit,
        'indicador': 'I',
        'codigo_adminitarativo':'1',
        'numero_contrato': row['numero_contrato'],
        'tipo_contrato': '1',
        'numero_acta': row['orden'],
        'fecha_acta': row['fecha'],
        'valor_acta': f"{str(row['valor']).split('.')[0]}.00",
        'valor_pagado':  f"{str(row['valor']).split('.')[0]}.00",
        'porcentaje': '100.00',
        'conlusion': 'ENTREGADO PARCIAL DE LOS PRODUCTOS CONTRATADOS',
    } for _, row in df.iterrows()])

    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato

def df_tipo_6(tipo_registro,nit, inicio_consecutivo):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': 'ID2139724657',
        'nit': nit,
        'indicador': 'NA',
        'codigo': '',
        'numero_contrato': '',
        'fecha_inicio': '',
        'codigo_entidad_aseguradora': '',
        'nit_aseguradora': '',
        'cuenta_poliza': '',
        'valor_asegurado': '',
        'fecha_consignacion': '',
        'fecha_vencimiento': ''
    }])

    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato


def df_tipo_7(tipo_registro,nit, inicio_consecutivo, df):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': row['id_recurso'],
        'nit': nit,
        'indicador': 'I',
        'codigo_adminitarativo': '5',
        'numero_contrato': row['numero_contrato'],
        'fecha_acta': row['fecha'].date(),
        'entidad':'1',
        'nit_entidad': row['banco'],
        'cuenta_bancaria': row['cuenta'],
        'rendimiento': str(row['rendimiento']),
        'fecha_pago_rendimiento': row['fecha_pago'].date(),
        'portafolio': row['portafolio'],
    } for _, row in df.iterrows()])

    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato


def main():

    PROPIEDADES_TIPO_1 = [
    # 1
    'NI',
    # 2
    '900091143',
    # 3
    '2025-10-01',
    # 4
    '2025-10-31',
    ]

    PROPIEDADES_TIPO_2 = [
    # 1
        ['ID2139724657'], # RESOLUCION
    # 2
        ['I'],
    # 3
        '4',
    # 4
        ['123753'],
    # 5
        ['2024-09-19'],
    # 6
        ['6012006700.00'],
    ]

    PROPIEDADES_TIPO_3 = [
    # 1
        'I',
    # 2|
        '1',
    # 3
        [
            'CONTRATAR LA PRESTACION DE SERVICIOS CON EL FIN DE LLEVAR A CABO LAS ACTIVIDADES DESCRITAS PARA LOS EQUIPOS BASICOS DE SALUD EN EL MICROTERRITORIO ASIGNADO',
            'ADQUISICION DE CHALECOS GORRAS Y MALETINES CON EMBLEMAS OIFICIALES PARA IDENTIFICAR A LOS MIEMBROS DE LOS EQUIPOS BASICOS DE SALUD',
            'PRESTACION DE SERVICIOS DE TRANSPORTE ESPECIAL PARA EL DESARROLLO DE LA ESTRATEGIA DE ATENCION',
            'COMPRA DE EQUIPOS BIOMEDICOSPARA EQUIPOS BASICOS EN SALUD DE LAS DIFERENTES SEDES QUE CONFORMAN LAS REDES PRESTADORAS DE SERVICIOS EN SALUD',
            'PRESTACION DE SERVICIOS PARA EL APOYO A LA GESTION COMO COORDINACION TECNICA ADMINISTRATIVA CON EL FIN DE APOYAR A LA EJECUCION OPERATIVA DEL PROYECTO FORTALECIMIENTO DE LA GESTION TERRITORIAL EN APS',
            'COMPRA DE ELEMENTOS CATEGORIZADOS EN LE LINEA DE CONSUMO MATERIALES Y SUMINISTROS DE ACUERDO A LAS NECESIDADES IDENTIFICADAS PARA EL DESARROLLO DEL PROGRAMA DE ATENCION PRIMARIA EN SALUD DE LOS EQUIPOS BASICOS DE SALUD EN LAS DIFERENTES ZONAS Y MICROTERRITORIOS RURALES Y URBANOS DEL MUNICIPIO DE PASTO EJECUTADO POR PASTO SALUD ESE',
            'COMPRA DE MATERIAL PUBLICITARIO DE IDENTIFICACION DEL PERSONAL DE EQUIPOS BASICOS EN SALUD DENTRO DEL DESARROLLO DE ESTRATEGIAS DE ATENCION PRIMARIA EN SALUD EN LAS DIFERENTES ZONAS Y MICROTERRITORIOS RURALES Y URBANOS DEL MUNICIPIO DE PASTO EJECUTADO POR PASTO SALUD ESE.'
        ]
    ]

    url = 'activos/reporte_ser/RESOLUSION_1397_TIPO_3.xlsx'
    ur_polisa = 'activos/polisa_1397.xlsx'
    url_flujo = 'activos/reporte_ser/RESOLUSION_1397_TIPO_5.xlsx'
    url_rendimiento = 'activos/rendimientos_1397.xlsx'


    cruzadfas = 'activos/cruzadas.xlsx'
    url_flujo_2025 = 'activos/flujo_caja-limpio.xlsx'
    url_consolidado_1773 = 'activos/consolidado_1773.xlsx'
    sumar_valores = 'activos/ap.xlsx'

    # Read the Excel file
    df = pd.read_excel(url, engine='openpyxl')  # Ensure `openpyxl` is installed
    df_recurso_4 = pd.read_excel(ur_polisa)  # Ensure `openpyxl` is installed
    df_recurso_5 = pd.read_excel(url_flujo, engine='openpyxl')  # Ensure `openpyxl` is installed
    df_recurso_7 = pd.read_excel(url_rendimiento, engine='openpyxl')  # Ensure `openpyxl` is installed

    #df_group_by_numero = df.groupby('numero_contrato')['valor'].sum().reset_index()
    #df_group_by_numero.to_excel('activos/consolidado_1778_final.xlsx', index=False)
    #df_contratos_presentes_en_caja_2025['diferente'] =  df_contratos_presentes_en_caja_2025['valor'] == df_contratos_presentes_en_caja_2025['valor_pagado']

    #df_group_by_numero.to_excel('activos/nueva_cruzada_restantes_1778.xlsx', index=False)
    #df_recurso_5.to_excel('activos/flujo_caja_limpio.xlsx', index=False)

    #df_recurso_5['numero_contrato'] = \
    #df_recurso_5['numero_contrato'].str.replace(' ', '').str.extract(r'(\d{3,4}-\d{4})')[0]
    df_recurso_5['fecha'] = pd.to_datetime(df_recurso_5['fecha'],
                                                             errors='coerce').dt.date

    #df_recurso_5.to_excel('activos/flujo_caja_limpio_3.xlsx', index=False)



# FALTRAIA FRECUENCIA DE PAGO EN EL TIPO 4
# SEGUROS

    # 0
    TIPO_REGISTROS = ['1','2', '3','4','5','6','7']
    # 3
    FE_CORTE = datetime.now().strftime('%Y-%m-%d')


    tipo_2 = df_tipo_2(TIPO_REGISTROS[1], PROPIEDADES_TIPO_2, PROPIEDADES_TIPO_1[1])
    tipo_3 = df_tipo_3(TIPO_REGISTROS[2],len(tipo_2),PROPIEDADES_TIPO_3,df,PROPIEDADES_TIPO_1[1])  # Suponiendo que no hay registros de tipo 3 por ahora
    tipo_4 = df_tipo_4(TIPO_REGISTROS[3], df_recurso_4, PROPIEDADES_TIPO_1[1], len(tipo_2) + len(tipo_3))  # Suponiendo que no hay registros de tipo 4 por ahora
    tipo_5 = df_tipo_5(TIPO_REGISTROS[4], df_recurso_5, PROPIEDADES_TIPO_1[1],len(tipo_2) + len(tipo_3) + len(tipo_4))  # Suponiendo que no hay registros de tipo 5 por ahora
    tipo_6 = df_tipo_6(TIPO_REGISTROS[5], PROPIEDADES_TIPO_1[1],len(tipo_2) + len(tipo_3) + len(tipo_4) + len(tipo_5))
    tipo_7 = df_tipo_7(TIPO_REGISTROS[6], PROPIEDADES_TIPO_1[1],len(tipo_2) + len(tipo_3) + len(tipo_4) + len(tipo_5) + len(tipo_6),df_recurso_7)
    # Suponiendo que no hay registros de tipo 6 por ahora
    tipo_1 = df_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1,
                        len(tipo_2) + len(tipo_3) + len(tipo_4) + len(tipo_5) + len(tipo_6) + len(tipo_7))

    consolidado = codificar_formato(tipo_1) + '\n'
    consolidado += codificar_formato(tipo_2) + '\n'
    consolidado += codificar_formato(tipo_3) + '\n'
    consolidado += codificar_formato(tipo_4) + '\n'
    consolidado += codificar_formato(tipo_5) + '\n'
    consolidado += codificar_formato(tipo_6) + '\n'
    consolidado += codificar_formato(tipo_7)

    file_name = f"reportes/SER124DREC20251031NI000900091143{PROPIEDADES_TIPO_2[0][0]}.txt"

    # Guardar el archivo en la misma carpeta
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(consolidado)


if __name__ == "__main__":
    main()