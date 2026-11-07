import re
import unicodedata
import pandas as pd


def limpiar_texto(texto):
    """Limpia tildes, caracteres especiales, saltos de línea y convierte a mayúsculas segun PISIS."""
    if pd.isna(texto) or texto is None:
        return ""
    texto_str = str(texto).strip()
    # Eliminar saltos de linea internos
    texto_str = re.sub(r'[\r\n]+', ' ', texto_str)
    # Normalizar Unicode para quitar tildes
    texto_nfd = unicodedata.normalize('NFD', texto_str)
    texto_sin_tildes = "".join(c for c in texto_nfd if unicodedata.category(c) != 'Mn')
    # Conservar solo caracteres alfanumericos y espacios
    texto_limpio = re.sub(r'[^a-zA-Z0-9\s\-._]', '', texto_sin_tildes)
    return texto_limpio.upper()


def formatear_fecha(valor, fecha_defecto="1900-01-01"):
    """Formatea la fecha a AAAA-MM-DD o retorna la fecha por defecto si no aplica."""
    if pd.isna(valor) or str(valor).strip() == "" or str(valor).strip() == "NaT":
        return fecha_defecto
    try:
        dt = pd.to_datetime(valor)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return fecha_defecto


def formatear_decimal(valor):
    """Formatea montos a decimales estricto sin comas de miles y 2 decimales."""
    if pd.isna(valor) or str(valor).strip() == "":
        return ""
    try:
        val_float = float(valor)
        return f"{val_float:.2f}"
    except ValueError:
        return ""


def formatear_porcentaje(valor):
    """Formatea el porcentaje con un solo decimal (ej: 100.0 o 20.5)."""
    if pd.isna(valor) or str(valor).strip() == "":
        return ""
    try:
        val_float = float(valor)
        return f"{val_float:.1f}"
    except ValueError:
        return "0.0"


def generar_archivo_plano():
    archivo_excel = '/home/esteban/Documents/APS-RICAURTE/Plantilla_Datos_SER124DREC_1397.xlsx'

    # 1. Cargar Hojas de Entrada
    xls = pd.ExcelFile(archivo_excel)
    df_params = pd.read_excel(xls, 'Parametros_Control')
    df_t3_in = pd.read_excel(xls, 'Tipo_3_Contratos')
    df_t4_in = pd.read_excel(xls, 'Tipo_4_Polizas')
    df_t5_in = pd.read_excel(xls, 'Tipo_5_Seguimientos')
    df_t6_in = pd.read_excel(xls, 'Tipo_6_Reintegros')
    df_t7_in = pd.read_excel(xls, 'Tipo_7_Rendimientos')

    param = df_params.iloc[0]

    # 2. CONSTRUIR REGISTRO TIPO 2 (Incorporación)
    val_incorp_raw = param.get('indicador_incorp')
    ind_t2 = 'NA' if pd.isna(val_incorp_raw) or str(val_incorp_raw).strip().lower() == 'nan' else str(
        val_incorp_raw).upper()

    registro_t2 = {
        'tipo_registro': '2',
        'consecutivo': 1,
        'id_recurso': str(param['id_recurso']),
        'nit': str(param['nit_reportante']),
        'indicador': ind_t2,
        'tipo_acto': str(param.get('tipo_acto_incorp', '')) if ind_t2 != 'NA' else '',
        'numero_acto': limpiar_texto(param.get('numero_acto_incorp', '')) if ind_t2 != 'NA' else '',
        'fecha_incorp': formatear_fecha(param.get('fecha_incorp', ''), "") if ind_t2 != 'NA' else '',
        'valor_incorp': formatear_decimal(param.get('valor_incorp', '')) if ind_t2 != 'NA' else ''
    }
    df_tipo_2 = pd.DataFrame([registro_t2])

    # 3. CONSTRUIR REGISTRO TIPO 3 (Contratos)
    registros_t3 = []
    consecutivo = 2
    for _, r in df_t3_in.iterrows():
        ind = str(r.get('indicador', 'I')).upper()
        es_na = ind == 'NA'
        registros_t3.append({
            'tipo_registro': '3',
            'consecutivo': consecutivo,
            'id_recurso': str(r['id_recurso']),
            'nit': str(param['nit_reportante']),
            'indicador': ind,
            'tipo_contrato': str(r.get('tipo_contrato', '')) if not es_na else '',
            'numero_contrato': limpiar_texto(r.get('numero_contrato', '')) if not es_na else '',
            'fecha_inicio': formatear_fecha(r.get('fecha_inicio_contrato')) if not es_na else '',
            'fecha_termino': formatear_fecha(r.get('fecha_termino_contrato')) if not es_na else '',
            'objeto': limpiar_texto(r.get('objeto', ''))[:500] if not es_na else '',
            'valor': formatear_decimal(r.get('valor_contratado')) if not es_na else '',
            'tipo_doc_contratista': str(r.get('contratista_tipo_identificacion', '')) if not es_na else '',
            'num_doc_contratista': str(r.get('numero_identificacion', '')).split('.')[0] if not es_na else '',
            'nombre_contratista': limpiar_texto(r.get('nombre_contratista', ''))[:100] if not es_na else '',
            'tipo_doc_supervisor': str(r.get('tipo_doc_supervisor', '')) if not es_na else '',
            'num_doc_supervisor': str(r.get('numero_supervisor', '')).split('.')[0] if not es_na else '',
            'nombre_supervisor': limpiar_texto(r.get('nombre_supervisor', ''))[:100] if not es_na else ''
        })
        consecutivo += 1
    df_tipo_3 = pd.DataFrame(registros_t3)

    # 4. CONSTRUIR REGISTRO TIPO 4 (Pólizas)
    registros_t4 = []
    for _, r in df_t4_in.iterrows():
        ind = str(r.get('indicador', 'NA')).upper()
        es_na = ind == 'NA'
        registros_t4.append({
            'tipo_registro': '4',
            'consecutivo': consecutivo,
            'id_recurso': str(r['id_recurso']),
            'nit': str(param['nit_reportante']),
            'indicador': ind,
            'tipo_contrato': str(r.get('codigo_tipo_contrato', '')) if not es_na else '',
            'numero_contrato': limpiar_texto(r.get('numero_contrato', '')) if not es_na else '',
            'poliza': limpiar_texto(r.get('poliza', '')) if not es_na else '',
            'fecha_poliza': formatear_fecha(r.get('fecha')) if not es_na else ''
        })
        consecutivo += 1
    df_tipo_4 = pd.DataFrame(registros_t4)

    # 5. CONSTRUIR REGISTRO TIPO 5 (Seguimiento Técnico / Financiero)
    registros_t5 = []
    for _, r in df_t5_in.iterrows():
        ind = str(r.get('indicador', 'I')).upper()
        es_na = ind == 'NA'
        registros_t5.append({
            'tipo_registro': '5',
            'consecutivo': consecutivo,
            'id_recurso': str(r['id_recurso']),
            'nit': str(param['nit_reportante']),
            'indicador': ind,
            'tipo_contrato': str(r.get('codigo_tipo_contrato', '')) if not es_na else '',
            'numero_contrato': limpiar_texto(r.get('numero_contrato', '')) if not es_na else '',
            'tipo_acta': str(r.get('codigo_tipo_acta', '')) if not es_na else '',
            'num_acta': str(r.get('orden', '')).split('.')[0] if not es_na else '',
            'fecha_acta': formatear_fecha(r.get('fecha')) if not es_na else '',
            'valor_obligado': formatear_decimal(r.get('valor_obligado')) if not es_na else '',
            'valor_pagado': formatear_decimal(r.get('valor_pagado')) if not es_na else '',
            'porcentaje': formatear_porcentaje(r.get('porcentaje_ejecucion', 0)) if not es_na else '',
            'conclusion': limpiar_texto(r.get('conclusion', ''))[:500] if not es_na else ''
        })
        consecutivo += 1
    df_tipo_5 = pd.DataFrame(registros_t5)

    # 6. CONSTRUIR REGISTRO TIPO 6 (Reintegro de Recursos)
    registros_t6 = []
    for _, r in df_t6_in.iterrows():
        ind = str(r.get('indicador', 'NA')).upper()
        es_na = ind == 'NA'
        registros_t6.append({
            'tipo_registro': '6',
            'consecutivo': consecutivo,
            'id_recurso': str(r['id_recurso']),
            'nit': str(param['nit_reportante']),
            'indicador': ind,
            'tipo_acto': str(r.get('tipo_acto', '')) if not es_na else '',
            'numero_acto': limpiar_texto(r.get('numero_acto', '')) if not es_na else '',
            'fecha_acto': formatear_fecha(r.get('fecha_acto')) if not es_na else '',
            'entidad_reintegro': str(r.get('codigo_entidad_reintegro', '')) if not es_na else '',
            'nit_banco': str(r.get('nit_banco', '')).split('.')[0] if not es_na else '',
            'cuenta_bancaria': str(r.get('numero_cuenta', '')) if not es_na else '',
            'valor_reintegrado': formatear_decimal(r.get('valor_reintegrado')) if not es_na else '',
            'fecha_consignacion': formatear_fecha(r.get('fecha_consignacion')) if not es_na else '',
            'portafolio': str(r.get('portafolio', '')) if not es_na else ''
        })
        consecutivo += 1
    df_tipo_6 = pd.DataFrame(registros_t6)

    # 7. CONSTRUIR REGISTRO TIPO 7 (Rendimientos Financieros)
    registros_t7 = []
    for _, r in df_t7_in.iterrows():
        ind = str(r.get('indicador', 'NA')).upper()
        es_na = ind == 'NA'
        registros_t7.append({
            'tipo_registro': '7',
            'consecutivo': consecutivo,
            'id_recurso': str(r['id_recurso']),
            'nit': str(param['nit_reportante']),
            'indicador': ind,
            'tipo_acto': str(r.get('tipo_acto', '')) if not es_na else '',
            'numero_acto': limpiar_texto(r.get('numero_acto', '')) if not es_na else '',
            'fecha_acto': formatear_fecha(r.get('fecha_acto')) if not es_na else '',
            'entidad_reintegro': str(r.get('codigo_entidad_reintegro', '')) if not es_na else '',
            'nit_banco': str(r.get('nit_banco', '')).split('.')[0] if not es_na else '',
            'cuenta_bancaria': str(r.get('numero_cuenta', '')) if not es_na else '',
            'valor_rendimientos': formatear_decimal(r.get('valor_rendimientos')) if not es_na else '',
            'fecha_consignacion': formatear_fecha(r.get('fecha_consignacion')) if not es_na else '',
            'portafolio': str(r.get('portafolio', '')) if not es_na else ''
        })
        consecutivo += 1
    df_tipo_7 = pd.DataFrame(registros_t7)

    # 8. CONSTRUIR REGISTRO TIPO 1 (Control)
    total_registros_detalle = len(df_tipo_2) + len(df_tipo_3) + len(df_tipo_4) + len(df_tipo_5) + len(df_tipo_6) + len(
        df_tipo_7)
    df_tipo_1 = pd.DataFrame([{
        'tipo_registro': '1',
        'tipo_identificacion': str(param['tipo_identificacion']),
        'numero_identificacion': str(param['nit_entidad']),
        'fecha_inicial': formatear_fecha(param['fecha_inicial']),
        'fecha_final': formatear_fecha(param['fecha_final']),
        'total_registros': total_registros_detalle
    }])

    # 9. CONVERTIR CADA TIPO A TEXTO PLANO SEPARADO POR PIPES (|)
    lineas = []
    for df in [df_tipo_1, df_tipo_2, df_tipo_3, df_tipo_4, df_tipo_5, df_tipo_6, df_tipo_7]:
        for row in df.itertuples(index=False):
            val_limpios = ["" if pd.isna(val) or str(val).lower() == 'nan' else str(val) for val in row]
            lineas.append("|".join(val_limpios))

    # SOLUCIÓN CLAVE: Unir con \r\n pero asegurando que la última línea NO lleve salto al final
    contenido_plano = "\r\n".join(lineas)

    # 10. GENERAR NOMBRE ESTÁNDAR PISIS (48 caracteres exactos)
    fecha_corte_str = pd.to_datetime(param['fecha_final']).strftime('%Y%m%d')
    tipo_id = str(param['tipo_identificacion']).upper().zfill(2)
    num_id = str(param['nit_entidad']).zfill(12)
    id_recurs = str(param['id_recurso'])

    nombre_archivo_txt = f"/home/esteban/Documents/APS-RICAURTE/SER124DREC{fecha_corte_str}{tipo_id}{num_id}{id_recurs}.txt"

    # Escribir sin salto de línea extra al final del archivo
    with open(nombre_archivo_txt, 'w', encoding='utf-8', newline='') as f:
        f.write(contenido_plano)

    print(f"✅ Archivo plano generado exitosamente: {nombre_archivo_txt}")
    print(f"📊 Registros totales de detalle procesados: {total_registros_detalle}")


if __name__ == "__main__":
    generar_archivo_plano()