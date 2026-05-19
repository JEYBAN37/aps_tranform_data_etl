import logging
import os
import re
from types import NoneType

import mysql
import pandas as pd
import numpy as np
from datetime import datetime
import unicodedata
import mysql.connector
from db_dtypes.pandas_backports import nanall

from credenciales import DRIVER_MYSQL, MYSQL_REPLICA_USER, MYSQL_APS, DRIVER_PATH, MYSQL_REPLICA_PASSWORD, \
    DATABASE_APS2024, DATABASE_APS2025, URL_CONTRATACION_PLANTILLA
from export_usuarios_institucionales import codificar_formato
from mysql_conector import ejecutar_consulta_mysql
from propiedades_aps124 import DISCAPACIDAD, ANIMALES_PERMITIDO, NIVEL_ESTUDIO, ETNIA, AFILIACION, \
    ENFERMEDADES_CRONICAS, TIPO_REGISTROS, PROPIEDADES_TIPO_2, PROPIEDADES_TIPO_1, FECHA_INICIAL, \
    FECHA_FINAL, COLUMNAS_PERSONAS_JOVENADULTO, traer_joven_adultos, MICROTERRITORIO, TERRITORIO, \
    query_familias


def limpiar_tildes(texto):
    try:
        print("paso tildes")
        if texto is None or pd.isna(texto):
            return ''
        texto = str(texto).strip()  # Remove leading and trailing spaces
        texto = ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        )
        return re.sub(r'[-:.;,#=´_•²⁰É¨·º¿°"—}^ª$@{&✓*+!?)(|/Ñ\s]', '', texto)
    except (ValueError, TypeError, NoneType):
        return 'REVISAR'


def limpiar_formato_longitud(valor, valor_por_defecto=np.nan):
    try:
        valor = str(valor).replace(',', '.')
        valor = valor.replace('-', '')  # Eliminar cualquier signo negativo existente
        valor_float = float(valor)

        # Verificar que tenga 6 o más dígitos
        if len(str(valor_float).replace('.', '')) < 6:
            return valor_por_defecto

        # Truncar a 10 caracteres si la longitud es 12
        valor_str = str(valor_float)
        if len(valor_str) >= 10:
            valor_float = float(valor_str[:9])

        # Asegurar que esté dentro del rango válido
        if -180 <= valor_float <= 180:
            # Verificar que tenga más de 6 dígitos después del punto
            partes = str(valor_float).split('.')
            if len(partes) < 2 or len(partes[1]) < 4:
                return valor_por_defecto

            # Verificar que el valor antes del punto sea exactamente 77
            if partes[0] != '77':
                return valor_por_defecto

            # Agregar un signo negativo a todos los valores
            valor_float = -abs(valor_float)

            # Asegurar que tenga máximo 2 números antes del punto
            if len(partes[0].replace('-', '')) > 2:
                return valor_por_defecto
            return str(valor_float)
        else:
            return valor_por_defecto
    except Exception:
        return valor_por_defecto


def limpiar_formato_latitud(valor, valor_por_defecto=np.nan):
    try:
        valor = str(valor).replace(',', '.')
        valor = valor.replace('-', '')  # Eliminar cualquier signo negativo existente

        valor_float = float(valor)

        # Verificar que tenga 6 o más dígitos
        if len(str(valor_float).replace('.', '')) < 6:
            return valor_por_defecto

        # Asegurar que esté dentro del rango válido
        if -90 <= valor_float <= 90:
            # Verificar que tenga más de 6 dígitos después del punto
            partes = str(valor_float).split('.')
            if len(partes) < 2 or len(partes[1]) < 5:
                return valor_por_defecto

            # Asegurar que tenga máximo 1 número antes del punto
            if len(partes[0].replace('-', '')) > 1:
                valor_float = float(partes[0][-1:] + '.' + partes[1])
                # recortar maxiomo 7 digitos despues del punto
            return str(float(f"{valor_float:.6f}"))
        else:
            return valor_por_defecto
    except Exception:
        return valor_por_defecto


def registro_tipo_1(tipo_registro, propiedades, fecha_inicial, fecha_final, num_total_registros):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': propiedades[0],
        'numero_identificacion': propiedades[1],
        'fecha_inicial': fecha_inicial,
        'fecha_final': fecha_final,
        'numero_total_registros': num_total_registros,
    }])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()

    return formato


def contador_nomenclatura(_, param, estado={'prev_param': None, 'contador': 0}):
    if param != estado['prev_param']:
        estado['contador'] = 0
    estado['contador'] += 1
    estado['prev_param'] = param
    return f'{estado["contador"]:04}'


def contador_nomenclatura_familia(_, sociambientales):
    if sociambientales is None or len(sociambientales) == 0:
        return f'{1:04}'
    return f'{1:04}'


def obtener_nuevo_contador(micro_anterior, micro_actual, contador_acumulado):
    # Si el microterritorio cambió, reiniciamos a 1
    if micro_actual != micro_anterior:
        return 1
    # Si es el mismo, sumamos 1 al que ya traíamos
    return contador_acumulado + 1


def contador_nomenclatura_hogar(_):
    return f'CF{_ + 1:03}'


def contador_nomenclatura_familia_hogar(_):
    return f'F{_ + 1:04}'


def convertidor_tipo_cedulas(param):
    print("paso tipo cedula")
    try:
        if pd.isna(param) or param.strip() == '' or param is None or param == 'None':
            return 'CC'
        param = param.strip().upper()
        if (param == 'PTT' or param == 'PPT'):
            return 'PT'
        elif (param == 'CC'):
            return 'CC'
        elif (param == 'TI'):
            return 'TI'
        elif (param == 'RC'):
            return 'RC'
        else:
            return 'CC'
    except Exception:
        return 'CC'


def convertidor_vivienda(param):
    try:
        param = int(param.split('.')[0])
        print("paso vivienda")
        return param
    except Exception:
        return 12


def convertidor_material(param, default):
    try:
        if param is None:
            raise ValueError("param es None")

        # Convertir a string por si viene como número o algo raro
        param = str(param)

        # Partir antes del punto y convertir a int
        value = int(param.split('.')[0])


        print("paso material")
        if value >= 1 :
            return value
        else:
            return default

    except Exception:
        return default


def convertidor_calculo_familiograma(param, default):
    try:
        param = int(param.split('.')[0])

        if 1 <= param <= 3:
            return param
        return int(default)
    except Exception:
        return default


def convertir_animales(param):
    try:
        print("paso animales")
        # Split the input string by the separator (e.g., "_")
        animales = param.lower().split("_")

        # Map each animal name to its corresponding code
        codigos = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in animales if
                   animal.strip() in ANIMALES_PERMITIDO]

        if not codigos:
            return 13

        # Concatenate the codes with commas
        return ",".join(codigos)
    except Exception:
        return 13


def contar_animales(param, num_1, num_2):
    contador = 0
    try:
        if param == '13':
            return contador

        num_1 = num_1.lower().split("_")
        codigo = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in num_1 if animal.strip() in ANIMALES_PERMITIDO]

        if codigo != '13':
            contador += 1

            num_2 = num_2.lower().split("_")
            codigo = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in num_2 if
                      animal.strip() in ANIMALES_PERMITIDO]
            if codigo != '13':
                contador += 1

            return contador
    except Exception:
        return 0


def calculo_apgar(param):
    try:
        param = int(param)
        if 17 <= param:
            return 1
        elif 13 <= param <= 16:
            return 2
        elif 9 <= param <= 12:
            return 3
        elif 0 <= param <= 8:
            return 4
        return param
    except (ValueError, TypeError):
        return 1


def calculo_variables_segun_zarit(param):
    try:
        param = int(param)
        if param <= 100:
            return 1
        elif param <= 45:
            return 2
        elif param > 100:
            return 1
        return param
    except (ValueError, TypeError):
        return 1


def calculo_zarit(param):
    try:
        param = int(param)
        if param <= 46:
            return 1
        elif 47 <= param <= 55:
            return 2
        elif param <= 100 or param > 100:
            return 3
        return param
    except (ValueError, TypeError):
        return 1


def convertidor_poblacion_vulnerable(param, poblacion):
    try:
        # quitar tildes
        param = limpiar_tildes(param).lower()

        if param == '':
            return 2

        if param == 'otro':
            return 2

        array = param.lower().split()

        if poblacion in array:
            return 1
        return 2
    except Exception:
        return 2


def limpiar_formato_microterritorio(param):
    if pd.isna(param):
        return ''
    s = str(param).strip()
    # Replace all '0' with 'MT' and remove all dots
    s = s.replace('0', '').replace('.', '')
    return f"MT0{s}"


def limpiar_estrato(param):
    try:
        if pd.isna(param):
            return '3'
        param = str(param).strip().split('.')[
            0]  # Convert to string, strip spaces, and take the part before any decimal point
        if param.isdigit() and 1 <= int(param) <= 6:
            return param
        return '3'
    except Exception:
        return '3'


def registro_tipo_2( df_info_general):
    # ajuste de microterritorio y territorio
    df_info_general['microterritorio'] = df_info_general['microterritorio'].apply(limpiar_formato_microterritorio)
    df_info_general['nombre_barrio'] = df_info_general['nombre_barrio'].apply(
        lambda x: re.split(r' T\d', str(x))[0].strip() if x is not None else x
    )

    df_info_general['estrato'] = df_info_general['estrato'].apply(limpiar_estrato)

    df_info_general['docr'] = df_info_general['docr'].apply(
        lambda x: str(x).strip().split('.')[0] if pd.notna(x) and str(x).strip() != '' else x)
    df_info_general['id_sociambiental_db'] = df_info_general['id_sociambiental_db'].apply(
        lambda x: str(x).strip().split('.')[0] if pd.notna(x) and str(x).strip() != '' else x)

    df_info_general = df_info_general[~df_info_general['territorio'].isna()]

    # familias_x_territorio_micro = df_info_general.groupby(['territorio']).size().reset_index(name='conteo_familias')

    df_info_general = df_info_general[~df_info_general['docr'].isna()]

    df_info_general = df_info_general[df_info_general['docr'] != '0']

    df_info_general = df_info_general.sort_values(by=['territorio', 'microterritorio']).reset_index(drop=True)


    # Filtrar registros con NaN en longitud y latitud
    df_invalidos = df_info_general[df_info_general[['longitud', 'latitud']].isna().any(axis=1)]

    # Filtrar registros válidos (sin NaN en longitud y latitud)
    formato = df_info_general.dropna(subset=['longitud', 'latitud'])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].apply(
            lambda v: v.upper() if isinstance(v, str) and any(ch.isalpha() for ch in v) and pd.notna(v) else v)

    return formato, df_invalidos


def alimentos_obt(param):
    try:
        return str(param).strip().split('.')[0]
    except (ValueError, TypeError):
        return ''


def formatear_tipo_2(tipo_registro,df_info_general,propiedades,df_contratacion):

    lista_formato = []

    for i, (idx, row) in enumerate(df_info_general.iterrows()):

        # 1. Identificamos el anterior
        micro_anterior = df_info_general.iloc[i - 1]['microterritorio'] if i > 0 else row['microterritorio']

        id_sociambiental_anterior = df_info_general.iloc[i - 1]['id_sociambiental_db'] if i > 0 else row[
            'id_sociambiental_db']

        # 2. Actualizamos el contador acumulado
        # Si es la primera fila o cambió el microterritorio, vuelve a 1. Si no, suma 1.
        if i == 0 or row['microterritorio'] != micro_anterior:
            contador_hogar = 1
        else:
            contador_hogar += 1

        if i == 0 or row['id_sociambiental_db'] != id_sociambiental_anterior:
            contador_familia = 1
        else:
            contador_familia += 1


        contador_ficha = (i % 999) + 1

        # 3. Formateamos el contador para el string (0001, 0002...)
        contador_str_h = f"{contador_hogar:04}"
        contador_str_f = f"{contador_familia:04}"
        contador_str_fi = f"{contador_ficha:03}"

        # Equipo Basico
        equipo_basico = propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + str(
            row['microterritorio']) + 'EBS' + f'{1:03}'
        numero_hogar = f"{equipo_basico}H{contador_str_h}"
        numero_familia = f"{numero_hogar}F{contador_str_f}"
        numero_ficha = f"{numero_familia}CF{contador_str_fi}"

        # rol contratista

        contratista = df_contratacion[df_contratacion['identificacion_contratista'] == str(row['docr'])]
        if contratista.empty:
            logging.warning(f"No se encontró contratista para el documento {row['docr']}")
            perfil_contratista = 'OTRO'
        else:
            perfil_contratista = limpiar_tildes(contratista['rol_del_contratista'].iloc[0]).upper()

        datos_fila = {
            'id_familia_db': row['id_familia_db'],
            'tipo_registro': tipo_registro,
            'consentimiento': propiedades[0],
            'cod_departamento': propiedades[1],
            'cod_subregion': propiedades[2],
            'cod_municipio': propiedades[3],
            'cod_territorio': row['territorio'],
            'cod_microterritorio': row['microterritorio'],
            'nombre_territorio': str(row['nombre_barrio']).upper() if pd.notna(row['nombre_barrio']) else '',
            'direccion': limpiar_tildes(row['direccion']).upper(),
            'longitud': limpiar_formato_longitud(row['longitud'], valor_por_defecto=-77.281101),
            'latitud': limpiar_formato_latitud(row['latitud'], valor_por_defecto=1.213601),
            'referencia_ubicacion': '',
            'numero_id_hogar': numero_hogar,
            'numero_id_familia': numero_familia,
            'estrato': row['estrato'] if pd.notna(row['estrato']) and str(row['estrato']).isdigit() and 1 <= int(
                row['estrato']) <= 6 else '0',
            'numero_hogares': row['numerohogares'] if pd.notna(row['numerohogares']) and str(
                row['numerohogares']).isdigit() and int(row['numerohogares']) > 0 else '1',
            'numero_familias': row['numerohogares'] if pd.notna(row['numerohogares']) and str(
                row['numerohogares']).isdigit() and int(row['numerohogares']) > 0 else '1',
            'numero_personas': str(row['numerohabitantes']).split('.')[0],
            'equpo_basico': equipo_basico,
            'nit_prestador': propiedades[4],
            'tipo_documento_responsable': convertidor_tipo_cedulas(row['tipodocr']),
            'numero_documento_responsable': safe_str(row.get('docr')),
            'perfil': perfil_contratista,
            'codigo': numero_ficha,
            'fecha': pd.to_datetime(row['fecha']).strftime('%Y-%m-%d') if pd.notna(row['fecha']) else '',
            'tipo_vivienda': convertidor_vivienda(row['vivienda']),
            'tipo_vivienda_desc': '',
            'material': convertidor_material(row['pared'], '8'),
            'material_desc': '',
            'piso': convertidor_material(row['piso'], '6'),
            'piso_desc': '',
            'techo': convertidor_material(row['techo'], '8'),
            'techo_desc': '',
            'numero_dormitorios': row['dormitorios'] if pd.notna(row['dormitorios']) and str(
                row['dormitorios']).isdigit() and int(row['dormitorios']) >= 0 else '0',
            'hacinamiento': convertidor_material(row['hacinamiento'], '2'),
            'riesgo_vivienda': convertidor_material(row['riesgo'], '11'),
            'acceso_vivienda': convertidor_material(row['acceso'], '5'),
            'combustible': convertidor_material(row['combustible'], '8'),
            'vector': convertidor_material(row['vector'], '2'),
            'riesgo_externo': convertidor_material(row['riesgoexterno'], '19'),
            'riesgo_externo_desc': '',
            'actividad_economica': convertidor_material(row['actividad'], '2'),
            'mascotas': convertir_animales(row['mascotas']),
            'total_mascotas': contar_animales(convertir_animales(row['mascotas']), row['numeroPerros'],
                                              row['numeroGatos']),
            'numero_mascotas': '',
            'servicio_agua': convertidor_material(row['aguaservicio'], '13'),
            'servicio_agua_desc': '',
            'disposicion_excretas': convertidor_material(row['diposicionexcretas'], '8'),
            'disposicion_excretas_desc': '',
            'agua_residuales': convertidor_material(row['aguaresiduales'], '7'),
            'agua_residuales_desc': '',
            'recoleccion_basura': convertidor_material(row['basura'], '6'),
            'recoleccion_basura_desc': '',
            'tipo_familia': convertidor_material(row['tipofamilia'], '1'),
            'numero_personas_familia': row['numeropersonas'] if pd.notna(row['numeropersonas']) and str(
                row['numeropersonas']).isdigit() and int(row['numeropersonas']) > 0 else '1',
            'resultado_familiograma': convertidor_calculo_familiograma(row['resultadofamiliograma'], '3'),
            'calculo_apgar': calculo_apgar(row['calculoapgar']),
            'cuidador': propiedades[5],
            'calculozarit': calculo_zarit(row['calculozarit']),
            'codigo_ecomapa': convertidor_material(row['resultadoecomapa'], '1'),
            'ninos_ninas': convertidor_poblacion_vulnerable(row['poblacionvulnerable'],
                                                            'familia con niñas, niños y adolescentes'),
            'gestantes': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'gestantes'),
            'adultos_mayores': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'adultosmayores'),
            'victimas_conflicto': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'victima conflicto'),
            'poblacion_discapacidad': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'discapacidad'),
            'enfermedad_catastrofica': convertidor_poblacion_vulnerable(row['poblacionvulnerable'],
                                                                        'personas con enferemedades cronicas'),
            'enfermedad_trasmisible': '',
            'covivientes': '2',
            'familia_vulnerable': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'no'),
            'estilo_vida': convertidor_material(row['estilodevidapredominante'], '2'),
            'antecedente_enfermedad': convertidor_material(row['antecedenteenfermedad'], '2'),
            'antecedente_enfermedad_desc': '',
            'alimentos': alimentos_obt(row['alimentos']) if pd.notna(row['alimentos']) and row['alimentos'] != '' else '9',
            'alimentos_desc': '',
            'estilodevidapredominante': convertidor_material(row['estilodevidapredominante'], '2'),
            'recursos_potenciadores': '2',
            'cuidado_entornos': '2',
            'practicas_relaciones_sanas': calculo_variables_segun_zarit(row['calculozarit']),
            'redes_colectivas': convertidor_poblacion_vulnerable(row['programasocial'], 'no'),
            'autonomia_adulto_mayor': '1',
            'prevencion_higiene': convertidor_poblacion_vulnerable(row['programasocial'], 'si'),
            'saberes_ancestrales': '2',
            'derecho_salud': '1',
            'id_familia': f"{numero_hogar}{numero_familia}",

        }
        lista_formato.append(datos_fila)

    formato = pd.DataFrame(lista_formato)

    return formato


def covertir_sexo(param):
    if pd.isna(param) or param.strip() == '' or param is None or param == 'None':
        return '3'

    param = param.strip().upper()
    if param in ['M', 'MASCULINO', 'HOMBRE']:
        return '1'

    elif param in ['F', 'FEMENINO', 'MUJER']:
        return '2'

    else:
        return '3'


def definir_pregunta_dos_opciones(param, esperada):
    if pd.isna(param) or param.strip() == '' or param is None or param == 'None':
        return '2'

    param = param.strip().upper()
    if param == esperada:
        return '1'
    else:
        return '2'


def convertidor_multicampos(param, options, default='6'):
    try:
        # quitar tildes
        param = limpiar_tildes(param).upper()

        if param == '':
            return default

        if param is None or param == 'None':
            return default

        if param in options:
            return options[param] if isinstance(options, dict) else param
        return default
    except (ValueError, TypeError):
        return default


def convertidor_mapa(parama, mapa, default):
    try:
        if not parama or parama.strip() == '' or parama is None:
            return default
        if parama in mapa:
            return mapa[parama]
        return default
    except (ValueError, TypeError):
        return default


def evaluacion_poblacional(param, edad=None, discapacidad=None, gestante=None, riesgo_psicosocial=None,
                           sospecha_victima=None):
    try:
        param = limpiar_tildes(param).upper()
        discapacidad = limpiar_tildes(discapacidad).upper() if discapacidad else ''
        gestante = limpiar_tildes(gestante).upper() if gestante else ''
        edad = int(edad.split('.')[0])
        riesgo_psicosocial = limpiar_tildes(riesgo_psicosocial).upper()
        sospecha_victima = limpiar_tildes(sospecha_victima).upper()

        if param == '' or param is None or param == 'None':
            return '8'

        if edad < 18:
            return '1'

        if edad >= 60:
            return '3'

        if discapacidad in DISCAPACIDAD:
            return '4'

        if gestante == 'SI':
            return '2'

        opciones = ['NO', 'NO APLICA', 'SD', '']

        if riesgo_psicosocial not in opciones or sospecha_victima not in opciones:
            return '6'

        if param == 'ADULTEZ' or param == 'JUVENTUD' or param == 'NO APLICA':
            return '7'

        return '7'
    except Exception:
        return '8'


def limpiar_formato_tala(param):
    # con expresion regular quitar letras tildes comas puntos
    try:
        if pd.isna(param):
            return ''

        limpiar_documento = lambda x: re.sub(r'[a-zA-Z’Ñ_/.,\s]', '', str(x)).lstrip('0') or '0'

        # Uso
        resultado = limpiar_documento(param)

        # Ejemplo de uso:
        # "000123" -> "123"
        # "00.45"  -> ".45"

        if len(resultado) <= 2:
            param = resultado + '0'
        return resultado
    except ValueError:
        return ''


def limpiar_formato_peso(param):
    try:
        if pd.isna(param):
            return ''
        s = str(param).strip()
        s = s.replace(',', '.')
        # keep only digits and the dot
        resultado = re.sub(r'[^0-9\.]', '', s)
        if resultado == '':
            return ''
        if len(resultado) > 4:
            resultado = resultado.replace('.', '')
            completo = resultado[:-3] + '.' + resultado[-2:]
            return f"{float(completo):.1f}"
        # If multiple dots, keep the first and join the rest as decimals
        if resultado.count('.') > 1:
            parts = resultado.split('.')
            resultado = parts[0] + '.' + ''.join(parts[1:])
        if '.' in resultado:
            valor = float(resultado)
            return f"{valor:.1f}"
        # No dot present, preserve original heuristic
        if len(resultado) == 3:
            return f"{int(resultado) / 10:.1f}"
        elif len(resultado) <= 2:
            return resultado + '.0'
        elif resultado == '':
            return ''
        elif resultado == '0':
            return ''
        return resultado + '.0'
    except Exception:
        return ''


def enfermedades_cronicas(enfermedades):
    enfermedades_sin_tildes = {}
    for i in enfermedades:
        enfermedades_sin_tildes = {limpiar_tildes(i): '1'}

    return enfermedades_sin_tildes


def safe_str(val, default='REVISAR'):
    if pd.isna(val) or val is None:
        return default
    return str(val).strip()


def registros_tipo_3( df_personas, df_familias=None):
    # quitar los None
    df_personas = df_personas.replace('None', np.nan)

    # quitar none en familia_id
    df_personas = df_personas.dropna(subset=['familia_id'])

    df_personas = df_personas.drop_duplicates(subset=['numerodoc', 'familia_id'])

    # quitar filas cuyo primer_nombre sea None o cadena vacía (si existe la columna)
    if 'primer_nombre' in df_personas.columns:
        df_personas['primer_nombre'] = df_personas['primer_nombre'].astype(object).apply(
            lambda v: None if pd.isna(v) else str(v).strip())
        df_personas = df_personas.dropna(subset=['primer_nombre'])
        df_personas = df_personas[df_personas['primer_nombre'] != '']

    df_merged = pd.merge(
        df_personas,
        df_familias[['id_familia_db']],
        left_on='familia_id',
        right_on='id_familia_db',
        how='left',  # 👈 Trae todas las personas aunque no tengan familia
        validate="many_to_one"  # Cada persona pertenece a una sola familia
    )

    df_merged = df_merged.dropna(subset=['id_familia_db'])  # Eliminar personas sin familia asociada

    df_familias_validas_parar_numerar = df_merged['id_familia_db'].unique()

    return df_merged , df_familias_validas_parar_numerar


def formatear_tipo_3(tipo_registro, df_merged, familias_validas):

    df_a_formatear = pd.merge(
        df_merged,
        familias_validas[['id_familia_db','numero_id_hogar', 'numero_id_familia']],
        left_on='familia_id',
        right_on='id_familia_db',
        how='left',  # 👈 Trae todas las personas aunque no tengan familia
        validate="many_to_one"  # Cada persona pertenece a una sola familia
    )

    formato = pd.DataFrame([{
        'id_familia_db': row['id_familia_db_x'],
        'tipo_registro': tipo_registro,
        'primer_nombre': limpiar_tildes(row['primer_nombre']) if pd.notna(row['primer_nombre']) and row[
            'primer_nombre'] != '' else '',
        'segundo_nombre': limpiar_tildes(row['segundo_nombre']) if pd.notna(row['segundo_nombre']) and row[
            'segundo_nombre'] != '' else '',
        'primer_apellido': limpiar_tildes(row['primer_apellido']) if pd.notna(row['primer_apellido']) and row[
            'primer_apellido'] != '' else '',
        'segundo_apellido': limpiar_tildes(row['segundo_apellido']) if pd.notna(row['segundo_apellido']) and row[
            'segundo_apellido'] != '' else '',
        'tipo_documento': convertidor_tipo_cedulas(row.get('tipodoc')),
        'numero_documento': limpiar_tildes(safe_str(row.get('numerodoc'))),
        'fecha_nacimiento': pd.to_datetime(row['fechanac']).strftime('%Y-%m-%d') if pd.notna(row['fechanac']) else '',
        'sexo': covertir_sexo(row.get('sexo')),
        'gestante': definir_pregunta_dos_opciones(row.get('gestacion'), 'SI'),
        'rol': convertidor_multicampos(row.get('rol'), ['1', '2', '3', '4', '5', '6']),
        'ocupacion': convertidor_material(row.get('ocupacion'), '9998'),
        'estudio': convertidor_multicampos(row.get('estudio'), NIVEL_ESTUDIO, '3'),
        'regimen_afiliacion': convertidor_multicampos(limpiar_tildes(row.get('regimen')), AFILIACION, '5'),
        'eapb': '',
        'grupo_poblacional': evaluacion_poblacional(row.get('cursovida'), row.get('edad'), row.get('discapacidad'),
                                                    row.get('riesgopsicosocial'), row.get('sopechamaltrato')),
        'etnia': convertidor_multicampos(row.get('etnia'), ETNIA, '07'),
        'medico_tradicional': '',
        'pueblo_indigena': '',
        'condiciones_disca': convertidor_multicampos(limpiar_tildes(row.get('discapacidad')), DISCAPACIDAD, '8'),
        'peso': limpiar_formato_peso(row.get('peso')),
        'talla': limpiar_formato_tala(row.get('talla')),
        'peso_talla': '',
        'perimetro_branquial': '',
        'codiciones_cronicas': convertidor_multicampos(row.get('condicioncronica'),
                                                       enfermedades_cronicas(ENFERMEDADES_CRONICAS), '2'),
        'materno': '',
        'intervenciones_canalizaciones': '',
        'motivos_no_atencion': '',
        'practica_deporte': '',
        'lactancia_materna': '',
        'memses_lactancia': '',
        'signos_desnutricion': '',
        'gastrica_o_respiratoria': '',
        'gastrica_o_respiratoria_desc': '',
        'atencion_enfermedad_aguda': '',
        'motivos': '',
        'numero_id_hogar': safe_str(row.get('numero_id_hogar')) + safe_str(row.get('numero_id_familia')),
        'id_integrante': safe_str(row.get('numero_id_hogar')) + safe_str(row.get('numero_id_familia')) + safe_str(
            convertidor_tipo_cedulas(row.get('tipodoc'))) + safe_str(row.get('numerodoc')),
    } for _, row in df_a_formatear.iterrows()
    ])

    formato = formato.drop_duplicates(subset=['numero_documento'])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].apply(
            lambda v: v.upper() if isinstance(v, str) and any(ch.isalpha() for ch in v) and pd.notna(v) else v)

    # ordenar por id_familia_db
    formato = formato.sort_values(by=['id_familia_db']).reset_index(drop=True)

    # Check the columns in df_info_general
    return formato


def main():
    numero = limpiar_formato_longitud('-77.26670')
    numero_lat = limpiar_formato_latitud('1.203703282')

    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False  # Disable autocommit
    )

    try:
        cursor = connection.cursor()
        # familias_query = ejecutar_consulta_mysql(query_familias(TERRITORIO, MICROTERRITORIO) , cursor)
        #
        # if not familias_query:
        #     print("No se encontraron familias para el territorio y microterritorio especificados.")


        FE_REPORTE = datetime.now().strftime('%Y-%m-%d')

        # df_familias = pd.DataFrame(familias_query, columns=[
        #     'id_familia_db',
        #     'id_sociambiental_db',
        #     'latitud',
        #     'longitud',
        #     'direccion',
        #     'hacinamiento',
        #     'territorio',
        #     'microterritorio',
        #     'nombre_barrio',
        #     'estrato',
        #     'numerohogares',
        #     'numerohabitantes',
        #     'hogar',
        #     'tipodocr',
        #     'docr',
        #     'profesion',
        #     'fecha',
        #     'vivienda',
        #     'pared',
        #     'piso',
        #     'techo',
        #     'dormitorios',
        #     'riesgo',
        #     'acceso',
        #     'combustible',
        #     'vector',
        #     'riesgoexterno',
        #     'actividad',
        #     'mascotas',
        #     'numeroPerros',
        #     'numeroGatos',
        #     'aguaservicio',
        #     'diposicionexcretas',
        #     'aguaresiduales',
        #     'basura',
        #     'tipofamilia',
        #     'numeropersonas',
        #     'resultadofamiliograma',
        #     'calculoapgar',
        #     'calculozarit',
        #     'zaritfuncionalidad',
        #     'resultadoecomapa',
        #     'poblacionvulnerable',
        #     'riesgopsicosocial',
        #     'estilodevidapredominante',
        #     'antecedenteenfermedad',
        #     'saludalternativa',
        #     'alimentos',
        #     'programasocial',
        #     'higiene',
        #     'total_personas_cursos_vida',
        #     'estado'
        # ])
        #
        # df_familias.to_csv('familias_actual.csv', index=False)
        # exit()

        df_familias = pd.read_csv('familias_actual.csv')



        # toma algunos datos de observacion
        df_familias = df_familias.drop_duplicates(subset=['id_familia_db'])

        df_familias_sin_integrantes = df_familias[df_familias['total_personas_cursos_vida'] == 0]
        df_viviendas_sin_familias = df_familias[df_familias['estado'] != 'SOCIOAMBIENTAL_OK']

        df_familias = df_familias[df_familias['total_personas_cursos_vida'] > 0]

        df_familias = df_familias[df_familias['estado'] == 'SOCIOAMBIENTAL_OK']
        df_familias.loc[:, 'id_familia_db'] = df_familias['id_familia_db'].apply(lambda v: '' if pd.isna(v) else (
            str(int(float(v))) if re.match(r'^\s*\d+(\.0+)?\s*$', str(v)) else str(v).strip()))

        query_personas_adultas = ejecutar_consulta_mysql(traer_joven_adultos(), cursor)
        df_personas = pd.DataFrame(query_personas_adultas,
                                   columns=COLUMNAS_PERSONAS_JOVENADULTO)  # DataFrame vacío para personas, ya que no se usa en este ejemplo
        df_personas.loc[:, 'familia_id'] = df_personas['familia_id'].apply(lambda v: '' if pd.isna(v) else (
            str(int(float(v))) if re.match(r'^\s*\d+(\.0+)?\s*$', str(v)) else str(v).strip()))

        reportados = pd.read_csv('reportes/cedulas_reportadas.csv', dtype=str, keep_default_na=False)

        no_reportados = df_personas[~df_personas['numerodoc'].isin(reportados['cedula'])]

        # familias reportadas
        mask_missing = df_familias['longitud'].isna() | df_familias['latitud'].isna()
        df_familias.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (COORDENADAS INVALIDAS)'

        df_familias = df_familias.dropna(subset=['longitud', 'latitud'])

        df_familias_no_reportadas = df_familias[df_familias['id_familia_db'].isin(no_reportados['familia_id'])]

        postulados_tipo_2, falla_cordenadas = registro_tipo_2(df_familias_no_reportadas)


        df_familias_a_crear = postulados_tipo_2[postulados_tipo_2['id_familia_db'].isin(df_personas['familia_id'])]

        tipo_3_pre,tipo_2_ids = registros_tipo_3(df_personas, df_familias_a_crear)


        # familias que ya tienen integrantes
        postulados_tipo_2 = postulados_tipo_2[postulados_tipo_2['id_familia_db'].isin(tipo_2_ids)]

        # dar formato a tipo 2

        df_contratacion = pd.read_csv(URL_CONTRATACION_PLANTILLA)
        df_contratacion['identificacion_contratista'] = df_contratacion['identificacion_contratista'].apply(
            lambda x: str(x).strip().split('.')[0] if pd.notna(x) and str(x).strip() != '' else x)

        tipo_2 = formatear_tipo_2(TIPO_REGISTROS[1], postulados_tipo_2, PROPIEDADES_TIPO_2, df_contratacion)

        tipo_2.insert(2, 'consecutivo_registro', range(1, len(tipo_2) + 1))

        tipo_3 = formatear_tipo_3(TIPO_REGISTROS[2], tipo_3_pre, tipo_2)

        start_consec = len(tipo_2) + 1
        tipo_3.insert(2, 'consecutivo_registro', range(start_consec, start_consec + len(tipo_3)))


        tipo_1 = registro_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, FECHA_INICIAL, FECHA_FINAL,
                                 len(tipo_3) + len(tipo_2))


        tipo_3 = tipo_3.iloc[:, 1:]
        tipo_2 = tipo_2.iloc[:, 1:]

        consolidado = codificar_formato(tipo_1) + '\n'
        consolidado += codificar_formato(tipo_2) + '\n'
        consolidado += codificar_formato(tipo_3)

        os.makedirs(F'reportes/{FE_REPORTE}/{TERRITORIO}', exist_ok=True)

        reporte = pd.concat([tipo_1, tipo_2, tipo_3], ignore_index=True)

        falla_cordenadas.to_csv(
            F'reportes/{FE_REPORTE}/{TERRITORIO}/falla_cordenadas_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')

        reporte.to_csv(
            f'reportes/{FE_REPORTE}/{TERRITORIO}/reporte_aps124_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv',
            index=False)
        #responsables_malos.to_csv(
        #    F'reportes/{FE_REPORTE}/{TERRITORIO}/responsables_malos_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')

        df_familias_sin_personas = postulados_tipo_2[
            ~postulados_tipo_2['id_familia_db'].isin(df_personas['familia_id'])]

        df_familias_sin_personas.to_csv(
            F'reportes/{FE_REPORTE}/{TERRITORIO}/familias_sin_personas_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')



        file_name = f"reportes/{FE_REPORTE}/{TERRITORIO}/APS124CCFP{FECHA_INICIAL.replace('-', '')}NI000900091143.txt"

        # Guardar el archivo en la misma carpeta
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(consolidado)

        connection.commit()

    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()

# 1 | NI | 900091143 | 2025 - 12 - 22 | 2025 - 12 - 22 | 37442
