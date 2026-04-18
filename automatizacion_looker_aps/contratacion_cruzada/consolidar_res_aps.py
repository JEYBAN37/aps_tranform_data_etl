import re
from types import NoneType

import pandas as pd
import unicodedata

from automatizacion_looker_aps.contratacion_cruzada.extractores_texto import limpiar_numero_contrato

def limpiar_tildes(texto):
    try :
        print("paso tildes")
        if texto is None or pd.isna(texto):
            return ''
        texto = str(texto).strip()  # Remove leading and trailing spaces
        texto = ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        )
        return re.sub(r'[-:.;,#=_É·°"^ª$@&✓*+!?)(|/Ñ\s]', '', texto)
    except (ValueError, TypeError, NoneType):
        return 'REVISAR'

def definir_rol(objeto):
    objeto = limpiar_tildes(str(objeto).upper())

    if "MEDICINA" in objeto:
        return "Profesional en medicina"
    elif "AUXILIAR" in objeto:
        return "Auxiliares de enfermería"
    elif "ENFERMERIA" in objeto:
        return "Profesional en enfermería"
    elif "PROMOTOR" in objeto:
        return "Agente o gestor comunitario / promotor de salud"
    elif "ODONTOLOGO" in objeto:
        return "Profesional en  Odontología, Terapias, técnico"
    elif "ODONTOLOGIA" in objeto:
        return "Profesional en  Odontología, Terapias, técnico"
    elif "PSICOLOGIA" in objeto:
        return "Profesional en psicología"
    elif "PSCOLOGIA" in objeto:
        return "Profesional en psicología"
    elif "NUTRICION" in objeto:
        return "Profesional en Nutrición y Dietética"
    elif "TECNOLOGO" in objeto:
        return "Agente o gestor comunitario / promotor de salud"
    elif "TERAPEUTA" in objeto:
        return "Profesional en Terapias"
    elif "TERAPIA OCUPACIONAL" in objeto:
        return "Profesional en Terapias"
    elif " TERAPIA OCUPACIONAL " in objeto:
        return "Profesional en Terapias"
    elif "GESTOR" in objeto:
        return "Agente o gestor comunitario / promotor de salud"
    elif "TRANSPORTE" in objeto:
        return "Transporte"
    elif "COORDINACION TECNICA" in objeto:
        return "Personal de apoyo administrativo y de sistematización de información"
    elif "ESCARAPELA" in objeto:
        return "carnetización, emblemas de misión médica, distintivos, impresos"
    else:
        return "OTROS"

def formatear_fecha(fecha):
    if pd.isna(fecha):
        return None

    fecha = pd.to_datetime(fecha)
    return fecha.strftime('%d/%m/%Y')

def converti_pazo_meses(fecha_inicio, fecha_final):
    if pd.isna(fecha_inicio) or pd.isna(fecha_final):
        return None

    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_final = pd.to_datetime(fecha_final)

    meses = (fecha_final.year - fecha_inicio.year) * 12 + (fecha_final.month - fecha_inicio.month)

    if fecha_final.day >= fecha_inicio.day:
        meses += 1

    return meses

def valor_mensual(valor_contrato, plazo_ejecucion):
    if pd.isna(valor_contrato) or pd.isna(plazo_ejecucion) or plazo_ejecucion == 0:
        return None

    valor_contrato = float(valor_contrato)
    valor_mensual = valor_contrato / plazo_ejecucion
    return valor_mensual

def listar_contratos_bd():
    # 1. Carga de bases
    contratos_x_resolucion = pd.read_excel("bases/contratos_consolidado_real.xlsx", sheet_name='RES_17_4_2026')
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260327.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].apply(limpiar_numero_contrato)
    contratacion_juridica_2024 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2024")
    contratacion_juridica_2025 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2025")
    contratacion_juridica_2026 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2026")

    contratacion_juridica = pd.concat([contratacion_juridica_2024, contratacion_juridica_2025, contratacion_juridica_2026], ignore_index=True)


    df_contratos = contratos_x_resolucion.merge(secop, left_on="numero_contrato", right_on="Referencia del Proceso", how="left")
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()].drop_duplicates(["numero_contrato", "URLProceso"])[['resolucion','numero_de_proceso','nit','URLProceso','numero_contrato'
        ,'tipo_identificacion','identificacion_contratista','nombre_contratista','objeto','valor_contrato']]
    df_verificar_manualmente = df_contratos[df_contratos["URLProceso"].isna()].drop_duplicates(["numero_contrato", "URLProceso"])
    contratacion_juridica["NUMERO"] = contratacion_juridica["NUMERO"].apply(limpiar_numero_contrato)

    df_por_cargar =  df_por_descargar.merge(contratacion_juridica, left_on="numero_contrato", right_on="NUMERO", how="left")
    print("Contratos con URL para descargar:")

    formato = pd.DataFrame([{
        "nit": row['nit'],
        "resolucion": row['resolucion'],
        "numero_de_proceso": row['numero_de_proceso'],
        "enlace_secop": row['URLProceso'],
        "numero_contrato": row['numero_contrato'],
        "numero_cdp": row['No. DE CDP '],
        'numero_rp': row['No. DE R.P'],
        "tipo_identificacion": row['tipo_identificacion'],
        "identificacion_contratista": str(row['identificacion_contratista']),
        "nombre_contratista": row['nombre_contratista'],
        "rol_del_contratista" : definir_rol(row['OBJETO ']),
        "fecha_suscripcion_contrato": formatear_fecha(row['FECHA DE SUSCRIPCION (DD/MM/AAAA)']),
        "plazo_ejecucion": converti_pazo_meses(row['FECHA INICIAL (DD/MM/AAA)'], row['FECHA DE FINALIZACION (DD/MM/AAA)']),
        "fecha_inicio": formatear_fecha(row['FECHA INICIAL (DD/MM/AAA)']),
        "fecha_finalizacion": formatear_fecha(row['FECHA DE FINALIZACION (DD/MM/AAA)']),
        "objeto": row['OBJETO '],
        "valor_contrato": row['valor_contrato'],
        "valor_mensual": valor_mensual(row['valor_contrato'], converti_pazo_meses(row['FECHA INICIAL (DD/MM/AAA)'], row['FECHA DE FINALIZACION (DD/MM/AAA)']))
    } for _, row in df_por_cargar.iterrows()
    ])

    print(formato.head())

    formato.to_excel("bases/contratos_consolidado_prueba.xlsx", index=False)

if __name__ == "__main__":
    listar_contratos_bd()
    #limpiar_numero_contrato("084-2026")