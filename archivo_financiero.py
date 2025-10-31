from datetime import datetime
import pandas as pd

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

def convertidor_objeto(row, propiedades):
    for i, objeto in enumerate(propiedades):
        if  row == i:
            return objeto

def df_tipo_3(tipo_registro, inicio_consecutivo, propiedades_tipo_3, df_personas, nit):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'id_recurso': row['id_recurso'],
        'nit': nit,
        'indicador': row['indicador'],
        'tipo_contrato': propiedades_tipo_3[1],
        'numero_contrato': row['numero_contrato'],
        'fecha_inicio_contrato': row['fecha_inicio_contrato'].date(),
        'fecha_termino_contrato': row['fecha_termino_contrato'].date(),
        'objeto_contrato': convertidor_objeto(row['tipo_objeto'], propiedades_tipo_3[2]),
        'valor': f"{row['valor']}.00",
        'contratista_tipo_identificacion': row['contratista_tipo_identificacion'],
        'numero_identificacion': str(row['numero_identificacion']).split('.')[0] if pd.notna(row['numero_identificacion']) else '',
        'nombre_contratista': str(row['nombre_contratista']).upper() if pd.notna(row['nombre_contratista']) else '',
        'tipo_doc_supervisor': row['tipo_doc_supervisor'],
        'numero_supervisor': str(row['numero_supervisor']).split('.')[0] if pd.notna(row['numero_supervisor']) else '',
        'nombre_supervisor': str(row['nombre_supervisor']).upper() if pd.notna(row['nombre_supervisor']) else '',
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


def df_tipo_5(param, PROPIEDADES_TIPO_5, inicio_consecutivo):
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


def main():

    PROPIEDADES_TIPO_1 = [
    # 1
    'NI',
    # 2
    '900091143',
    # 3
    '2024-12-01',
    # 4
    '2024-12-31',
    ]

    PROPIEDADES_TIPO_2 = [
    # 1
        ['ID2177823635', 'ID2139724657'], # RESOLUCION
    # 2
        ['A', 'I'],
    # 3
        '4',
    # 4
        ['123201','123753'],
    # 5
        ['2024-03-04', '2024-09-19'],
    # 6
        ['4097080240.00', '6012006700.00'],
    ]

    PROPIEDADES_TIPO_3 = [
    # 1
        'I',
    # 2|
        '1',
    # 3
        [
            'CONTRATAR LA PRESTACION DE SERVICIOS CON EL FIN DE LLEVAR A CABO LAS ACTIVIDADES DESCRITAS PARA LOS EQUIPOS BASICOS DE SALUD EN EL MICROTERRITORIO ASIGNADO, EL DESARROLLO DE LA ESTRATEGIA DE ATENCION PRIMARIA EN SALUD, MEDIANTE LA VISITA DOMICILIARIA DE LA POBLACION DEL MUNICIPIO DE PASTO, IMPLEMENTANDO EL LINEAMIENTO EMITIDO POR EL MINISTERIO DE SALUD A RAIZ DE LA EXPEDICION DE LA RESOLUCION 2016 DEL 29 DE NOVIEMBRE DEL 2023 Y LA RESOLUCION 1778 DEL 31 DE OCTUBRE DE 2023 CONFORME A LAS OBLIGACIONES DESCRITAS EN EL NUMERAL 2.3 DE LOS PRESENTES ESTUDIOS DE CONVENIENCIA Y OPORTUNIDAD DE PASTO SALUD ESE',
            'ADQUISICION DE CHALECOS, GORRAS Y MALETINES CON EMBLEMAS OIFICIALES PARA IDENTIFICAR A LOS MIEMBROS DE LOS EQUIPOS BASICOS DE SALUD ',
            'PRESTACION DE SERVICIOS DE TRANSPORTE ESPECIAL PARA EL DESARROLLO DE LA ESTRATEGIA DE ATENCION',
            'COMPRA DE EQUIPOS BIOMEDICOSPARA EQUIPOS BASICOS EN SALUD DE LAS DIFERENTES SEDES QUE CONFORMAN LAS REDES PRESTADORAS DE SERVICIOS EN SALUD',
            'PRESTACION DE SERVICIOS PARA EL APOYO A LA GESTION COMO COORDINACION TECNICA ADMINISTRATIVA CON EL FIN DE APOYAR A LA EJECUCION OPERATIVA DEL PROYECTO FORTALECIMIENTO DE LA GESTION TERRITORIAL EN APS, EQUIPOS BASICOS DE SALUD: CONFORMACION OPERACION Y SEGUIMIENTO Y PRIMORDIALMENTE DEL CORRECTO FUNCIONAMIENTO DE LOS EQUIPOS BASICOS DE SALUD - EBS, PROPORCIONANDO EL PERSONAL DE APOYO PARA LA LOGISTICA Y SISTEMATIZACION DE LA INFORMACION Y FACILITAR LA ARTICULACION DE LAS INTERSECTORIALIDAD Y CONTINUIDAD EN LA ATENCION EN SALUD EN EL MUNICIPIO DE PASTO',
            'COMPRA DE ELEMENTOS CATEGORIZADOS EN LE LINEA DE CONSUMO MATERIALES Y SUMINISTROS DE ACUERDO A LAS NECESIDADES IDENTIFICADAS PARA EL DESARROLLO DEL PROGRAMA DE ATENCION PRIMARIA EN SALUD DE LOS EQUIPOS BASICOS DE SALUD EN LAS DIFERENTES ZONAS Y MICROTERRITORIOS RURALES Y URBANOS DEL MUNICIPIO DE PASTO EJECUTADO POR PASTO SALUD ESE ',
            'COMPRA DE MATERIAL PUBLICITARIO DE IDENTIFICACION DEL PERSONAL DE EQUIPOS BSICOS EN SALUD, DENTRO DEL DESARROLLO DE ESTRATEGIAS DE ATENCION PRIMARIA EN SALUD EN LAS DIFERENTES ZONAS Y MICROTERRITORIOS RURALES Y URBANOS DEL MUNICIPIO DE PASTO EJECUTADO POR PASTO SALUD ESE.'
        ]
    ]

    url = 'activos/costos.xlsx'
    ur_polisa = 'activos/polisa.xlsx'
    url_flujo = 'activos/flujo_caja-2024.xlsx'
    url_flujo_2025 = 'activos/flujo_caja-2025.xlsx'



    # Read the Excel file
    df = pd.read_excel(url, engine='openpyxl')  # Ensure `openpyxl` is installed
    df_recurso_4 = pd.read_excel(ur_polisa)  # Ensure `openpyxl` is installed
    df_contratos_presentes_en_caja = pd.read_excel(url_flujo,engine='openpyxl')
    df_contratos_presentes_en_caja_2025 = pd.read_excel(url_flujo_2025)# Ensure `openpyxl` is installed

    # limiar contrato
    df_contratos_presentes_en_caja['numero_contrato'] = df_contratos_presentes_en_caja['numero_contrato'].str.replace(' ','').str.extract(r'(\d{3,4}-\d{4})')[0]
    df_contratos_presentes_en_caja['fecha'] = pd.to_datetime(df_contratos_presentes_en_caja['fecha'], errors='coerce').dt.date
    df_contratos_presentes_en_caja_2025['numero_contrato'] = df_contratos_presentes_en_caja_2025['numero_contrato'].str.replace(' ','').str.extract(r'(\d{3,4}-\d{4})')[0]
    df_contratos_presentes_en_caja_2025['fecha'] = pd.to_datetime(df_contratos_presentes_en_caja_2025['fecha'], errors='coerce').dt.date

    df_caja_flujo_2024 = df_contratos_presentes_en_caja

    # limpiar dejar valores unicaos de caja
    df_contratos_presentes_en_caja = df_contratos_presentes_en_caja.drop_duplicates(subset=['numero_contrato'])

    # contratos que no estan en ninugna reeoslucion en caja 2024
    df_ninguna_Resolucion_pero_facturaron = df_contratos_presentes_en_caja[~df_contratos_presentes_en_caja['numero_contrato'].isin(df['numero_contrato'])].copy()

    # solo sacar la de la resolucion 1778
    df_resolucion_1778 = df[df['id_recurso'] == 'ID2177823635'].copy()
    df_resolucion_1397 = df[df['id_recurso'] == 'ID2139724657'].copy()

    df_flujo_1778 = df_resolucion_1778[df_resolucion_1778['numero_contrato'].isin(df_caja_flujo_2024['numero_contrato'])].copy()

    df_pagos_caja_1778 = df_caja_flujo_2024[
        df_caja_flujo_2024['numero_contrato'].isin(df_flujo_1778['numero_contrato'])].copy()

    # Agrupar el costo total por mes
    df_pagos_caja_1778['mes'] = pd.to_datetime(df_pagos_caja_1778['fecha']).dt.to_period('M')

    costo_total_por_mes = df_pagos_caja_1778.groupby('mes')['valor'].sum().reset_index()

    pagados_1778 = costo_total_por_mes[['valor']].sum()
    costo_total_a_pagar_1778 = df_flujo_1778[['valor_real']].sum()

    df_no_presentes_en_caja_1778 = df_resolucion_1778[~df_resolucion_1778['numero_contrato'].isin( df_contratos_presentes_en_caja['numero_contrato'])]
    suma_1778_no_pagadas = df_no_presentes_en_caja_1778[['valor']].sum(numeric_only=True)


    # calcular lo de la resolucion 1397
    df_flujo_1397 =  df_resolucion_1397[df_resolucion_1397['numero_contrato'].isin( df_caja_flujo_2024['numero_contrato'])].copy()
    df_pagos_caja_1397 = df_caja_flujo_2024[
        df_caja_flujo_2024['numero_contrato'].isin(df_flujo_1397['numero_contrato'])].copy()

    # Agrupar el costo total por mes
    df_pagos_caja_1397['mes'] = pd.to_datetime(df_pagos_caja_1397['fecha']).dt.to_period('M')

    pagados_1397 = df_pagos_caja_1397[['valor']].sum()
    costo_total_a_pagar_1397 = df_flujo_1397[['valor_real']].sum()


    suma_1397_personas_pagadas = df_flujo_1397[['valor_real']].sum(numeric_only=True)
    df_no_presentes_en_caja_1397 = df_resolucion_1397[~df_resolucion_1397['numero_contrato'].isin( df_contratos_presentes_en_caja['numero_contrato'])]
    suma_1397_no_pagadas = df_no_presentes_en_caja_1397[['valor']].sum(numeric_only=True)


    # no presentes de la resolucion 1397 y 1778
    df_no_presentes = pd.concat([df_no_presentes_en_caja_1778, df_no_presentes_en_caja_1397], ignore_index=True)
    df_contratos_2025 = df_contratos_presentes_en_caja_2025.drop_duplicates(subset=['numero_contrato'])


    df_presentes_2025 = df_no_presentes[df_no_presentes['numero_contrato'].isin(df_contratos_2025['numero_contrato'])].copy()
    df_revisar = df_no_presentes[~df_no_presentes['numero_contrato'].isin(df_contratos_2025['numero_contrato'])].copy()
    df_flujo_presentes_2025 = df_contratos_presentes_en_caja_2025[df_contratos_presentes_en_caja_2025['numero_contrato'].isin(df_presentes_2025['numero_contrato'])].copy()


    pagados_2025 = df_flujo_presentes_2025[['valor']].sum()
    costo_total_a_pagar_2025 = df_presentes_2025[['valor_real']].sum()


    dinero_esperado_a_pagar_resoluciones =  costo_total_a_pagar_2025 + costo_total_a_pagar_1397 + costo_total_a_pagar_1778
    dinero_pagado_hasta_fecha = pagados_2025 + pagados_1397 + pagados_1778


    # Identificar las filas donde aparece cada contrato
    df_flujo_1778['filas_contrato'] = df_flujo_1778.groupby('numero_contrato').cumcount() + 1

    df_malas = df_contratos_presentes_en_caja[df_contratos_presentes_en_caja['numero_contrato'].isna()].copy()


# FALTRAIA FRECUENCIA DE PAGO EN EL TIPO 4
# SEGUROS

    # 0
    TIPO_REGISTROS = ['1','2', '3','4','5']
    # 3
    FE_CORTE = datetime.now().strftime('%Y-%m-%d')


    tipo_2 = df_tipo_2(TIPO_REGISTROS[1], PROPIEDADES_TIPO_2, PROPIEDADES_TIPO_1[1])
    tipo_3 = df_tipo_3(TIPO_REGISTROS[2],len(tipo_2),PROPIEDADES_TIPO_3,df,PROPIEDADES_TIPO_1[1])  # Suponiendo que no hay registros de tipo 3 por ahora
    tipo_4 = df_tipo_4(TIPO_REGISTROS[3], df_recurso_4, PROPIEDADES_TIPO_1[1], len(tipo_2) + len(tipo_3))  # Suponiendo que no hay registros de tipo 4 por ahora
    tipo_5 = df_tipo_5(TIPO_REGISTROS[4], PROPIEDADES_TIPO_5, len(tipo_2) + len(tipo_3) + len(tipo_4))  # Suponiendo que no hay registros de tipo 5 por ahora

    tipo_1 = df_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, 100)




    print(consolidado)


if __name__ == "__main__":
    main()