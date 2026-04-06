import pandas as pd

def limpiar_numero_contrato(numero):
    # Eliminar espacios y caracteres no deseados
    numero_limpio = str(numero).strip()

    # Reemplazar los espacion en blanco
    numero_limpio = numero_limpio.replace(" ", "")

    # Aquí puedes agregar más reglas de limpieza si es necesario
    antes_Del_guion = numero_limpio.split("-")[0]  # Tomar solo la parte antes del guion
    if numero_limpio[0] == "0" and len(antes_Del_guion) > 3:  # Si el número comienza con "0" y tiene más de un dígito
        numero_limpio = numero_limpio[1:]
        return numero_limpio

    return numero_limpio

def definir_rol(objeto):
    objeto = str(objeto).upper()
    if "MEDICINA" in objeto:
        return "MEDICO"
    elif "AUXILIAR" in objeto:
        return "AUXILIAR DE ENFERMERIA"
    elif "ENFERMERIA" in objeto:
        return "JEFE DE ENFERMERIA"
    elif "PROMOTOR" in objeto:
        return "PROMOTOR DE SALUD"
    elif "ODONTOLOGO" in objeto:
        return "ODONTOLOGO"
    elif "PSICOLOGIA" in objeto:
        return "PSOCOLOGO"
    elif "NUTRICION" in objeto:
        return "NUTRICIONISTA"
    elif "TECNOLOGO" in objeto:
        return "TECNOLOGO EN SALUD"
    elif "TERAPEUTA" in objeto:
        return "FISIOTERAPEUTA"
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
    contratos_x_resolucion = pd.read_excel("bases/CONTRATOS_TODAS_RES_MAR_2026.xlsx")
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260327.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].apply(limpiar_numero_contrato)
    contratacion_juridica_2024 = pd.read_excel("bases/CONSOLIDADO BASE CONTRATACIÓN.xlsx", sheet_name="2024")
    contratacion_juridica_2025 = pd.read_excel("bases/CONSOLIDADO BASE CONTRATACIÓN.xlsx", sheet_name="2025")

    contratacion_juridica = pd.concat([contratacion_juridica_2024, contratacion_juridica_2025], ignore_index=True)


    df_contratos = contratos_x_resolucion.merge(secop, left_on="1.6", right_on="Referencia del Proceso", how="left")
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()].drop_duplicates(["1.6", "URLProceso"])[['1.6','1.3','1.2','URLProceso','1.11','1.12','1.13','1.7','1.8','1.9','1.10']]
    df_verificar_manualmente = df_contratos[df_contratos["URLProceso"].isna()].drop_duplicates(["1.6", "URLProceso"])

    df_por_cargar =  df_por_descargar.merge(contratacion_juridica, left_on="1.6", right_on="NUMERO", how="left")
    print("Contratos con URL para descargar:")

    formato = pd.DataFrame([{
        "nit": row['1.3'],
        "resolucion": row['1.2'],
        "numero_de_proceso": row['1.6'],
        "enlace_secop": row['URLProceso'],
        "numero_contrato": row['1.6'],
        "numero_cdp": row['No. DE CDP '],
        'numero_rp': row['No. DE R.P'],
        "tipo_identificacion": row['1.11'],
        "identificacion_contratista": str(row['1.12']),
        "nombre_contratista": row['1.13'],
        "rol_del_contratista" : definir_rol(row['1.9']),
        "fecha_suscripcion_contrato": formatear_fecha(row['FECHA DE SUSCRIPCION (DD/MM/AAAA)']),
        "plazo_ejecucion": converti_pazo_meses(row['1.7'], row['1.8']),
        "fecha_inicio": formatear_fecha(row['1.7']),
        "fecha_finalizacion": formatear_fecha(row['1.8']),
        "objeto": row['1.9'],
        "valor_contrato": row['1.10'],
        "valor_mensual": valor_mensual(row['1.10'], converti_pazo_meses(row['1.7'], row['1.8']))
    } for _, row in df_por_cargar.iterrows()
    ])

    print(formato.head())

    formato.to_excel("bases/contratos_consolidado.xlsx", index=False)

if __name__ == "__main__":
    listar_contratos_bd()
    #limpiar_numero_contrato("084-2026")