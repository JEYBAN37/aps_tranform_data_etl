import logging
import os
import re
import mysql
import pandas as pd
import numpy as np
from datetime import datetime
import unicodedata
import mysql.connector

from credenciales import DRIVER_MYSQL, MYSQL_REPLICA_USER, MYSQL_APS, DRIVER_PATH, MYSQL_REPLICA_PASSWORD, \
    DATABASE_APS2024
from export_usuarios_institucionales import codificar_formato
from mysql_conector import ejecutar_consulta_mysql
from propiedades_aps124 import DISCAPACIDAD, ANIMALES_PERMITIDO, NIVEL_ESTUDIO, ETNIA, AFILIACION, \
    ENFERMEDADES_CRONICAS, TIPO_REGISTROS, PROPIEDADES_TIPO_2, PROPIEDADES_TIPO_1, FECHA_INICIAL, \
    FECHA_FINAL, COLUMNAS_PERSONAS_JOVENADULTO, traer_joven_adultos, MICROTERRITORIO, TERRITORIO, \
    query_familias


def limpiar_tildes(texto):
    if texto is None or pd.isna(texto):
        return ''
    texto = str(texto).strip()  # Remove leading and trailing spaces
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    return re.sub(r'[-:.,#_·°"✓)(|/Ñ\s]', '', texto)

def limpiar_formato_longitud(valor):
    try:
        if pd.isna(valor):
            return np.nan
        valor = str(valor).replace(',', '.')
        valor = valor.replace('-', '')  # Eliminar cualquier signo negativo existente
        valor_float = float(valor)

        # Verificar que tenga 6 o más dígitos
        if len(str(valor_float).replace('.', '')) < 6:
            return np.nan

        # Truncar a 10 caracteres si la longitud es 12
        valor_str = str(valor_float)
        if len(valor_str) >= 10:
            valor_float = float(valor_str[:9])

        # Asegurar que esté dentro del rango válido
        if -180 <= valor_float <= 180:
            # Verificar que tenga más de 6 dígitos después del punto
            partes = str(valor_float).split('.')
            if len(partes) < 2 or len(partes[1]) < 4:
                return np.nan

            # Verificar que el valor antes del punto sea exactamente 77
            if partes[0] != '77':
                return np.nan

            # Agregar un signo negativo a todos los valores
            valor_float = -abs(valor_float)

            # Asegurar que tenga máximo 2 números antes del punto
            if len(partes[0].replace('-', '')) > 2:
                return np.nan
            return valor_float
        else:
            return np.nan
    except ValueError:
        return np.nan

def limpiar_formato_latitud(valor):
    try:
        if pd.isna(valor):
            return np.nan
        valor = str(valor).replace(',', '.')
        valor = valor.replace('-', '')  # Eliminar cualquier signo negativo existente

        valor_float = float(valor)

        # Verificar que tenga 6 o más dígitos
        if len(str(valor_float).replace('.', '')) < 6:
            return np.nan

        # Asegurar que esté dentro del rango válido
        if -90 <= valor_float <= 90:
            # Verificar que tenga más de 6 dígitos después del punto
            partes = str(valor_float).split('.')
            if len(partes) < 2 or len(partes[1]) < 5:
                return np.nan

            # Asegurar que tenga máximo 1 número antes del punto
            if len(partes[0].replace('-', '')) > 1:
                valor_float = float(partes[0][-1:] + '.' + partes[1])
            return valor_float
        else:
            return np.nan
    except ValueError:
        return np.nan

def registro_tipo_1(tipo_registro, propiedades, fecha_inicial,fecha_final, num_total_registros):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': propiedades[0],
        'numero_identificacion': propiedades[1],
        'fecha_inicial': fecha_inicial,
        'fecha_final' : fecha_final,
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

def contador_nomenclatura_familia(_):
    return f'{_+1:04}'

def contador_nomenclatura_hogar(_):
    return f'CF{_+1:03}'

def contador_nomenclatura_familia_hogar(_):
    return f'F{_+1:04}'

def convertidor_tipo_cedulas(param):
    if pd.isna(param) or param.strip() == '' or param is None or param == 'None':
        return 'CC'
    param = param.strip().upper()
    if(param == 'PTT'):
        return 'PT'
    elif(param == 'CC'):
        return 'CC'

def convertidor_vivienda(param):
    try:
        param = int(param.split('.')[0])
        return param
    except (ValueError, TypeError):
        return 12

def convertidor_material(param, default):
    try:
        param = int(param.split('.')[0])
        return param
    except (ValueError, TypeError):
        return int(default)

def convertidor_calculo_familiograma(param, default):
    try:
        param = int(param.split('.')[0])

        if 1 <= param <= 3:
            return param
        return int(default)
    except (ValueError, TypeError):
        return int(default)

def convertir_animales(param):
    try:
        # Split the input string by the separator (e.g., "_")
        animales = param.lower().split("_")

        # Map each animal name to its corresponding code
        codigos = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in animales if animal.strip() in ANIMALES_PERMITIDO]

        if not codigos:
            return 13

        # Concatenate the codes with commas
        return ",".join(codigos)
    except KeyError:
        return 13

def contar_animales(param , num_1 , num_2):
    contador = 0
    try:
       if param == '13':
        return contador

       num_1 = num_1.lower().split("_")
       codigo = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in num_1 if animal.strip() in ANIMALES_PERMITIDO]

       if codigo != '13' :
        contador += 1


        num_2 = num_2.lower().split("_")
        codigo = [str(ANIMALES_PERMITIDO[animal.strip()]) for animal in num_2 if animal.strip() in ANIMALES_PERMITIDO]
        if codigo != '13' :
         contador += 1

        return contador
    except (ValueError, TypeError):
        return 0

def calculo_apgar(param):
    param = int(param)
    try:
        if  17 <= param:
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
    param = int(param)
    try:
        if param <= 80:
            return 1
        elif param <= 45:
            return 2
        return param
    except (ValueError, TypeError):
        return 1

def calculo_zarit(param):
    param = int(param)
    try:
        if  param <= 46:
            return 1
        elif 47 <= param <= 55:
            return 2
        elif param <= 90:
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
    except (ValueError, TypeError):
        return 2

def registro_tipo_2(tipo_registro, propiedades, df_info_general):

    formato = pd.DataFrame([{
        'id_familia_db': row['id_familia_db'],
        'tipo_registro': tipo_registro,
        'consentimiento': propiedades[0],
        'cod_departamento': propiedades[1],
        'cod_subregion': propiedades[2],
        'cod_municipio': propiedades[3],
        'cod_territorio': row['territorio'],
        'cod_microterritorio': row['microterritorio'].replace('0', 'MT', 1),
        'nombre_territorio': row['nombre_barrio'].split('T')[0].strip() if 'T' in row['nombre_barrio'] else row['nombre_barrio'],
        'direccion': limpiar_tildes(row['direccion']),
        'longitud':limpiar_formato_longitud(row['longitud']),
        'latitud': limpiar_formato_latitud(row['latitud']),
        'referencia_ubicacion': '',
        'numero_id_hogar': propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}H' + contador_nomenclatura_familia(_),
        'numero_id_familia': propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}H{contador_nomenclatura_familia(_)}F{contador_nomenclatura_familia(_)}',
        'estrato': row['estrato'] if pd.notna(row['estrato']) and str(row['estrato']).isdigit() and 1 <= int(row['estrato']) <= 6 else '0',
        'numero_hogares': row['numerohogares'] if pd.notna(row['numerohogares']) and str(row['numerohogares']).isdigit() and int(row['numerohogares']) > 0 else '1',
        'numero_familias': row['numerohogares'] if pd.notna(row['numerohogares']) and str(row['numerohogares']).isdigit() and int(row['numerohogares']) > 0 else '1',
        'numero_personas': row['numerohabitantes'],
        #'equpo_basico':propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{_ + 1:03}',
        'equpo_basico':propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}',
        'nit_prestador': propiedades[4],
        'tipo_documento_responsable': convertidor_tipo_cedulas(row['tipodocr']),
        'numero_documento_responsable': row['docr'].replace(',', '').strip(),
        'perfil': limpiar_tildes(row['profesion']) if row['profesion'] != '' else 'OTRO',
        'codigo':propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}H' +contador_nomenclatura_familia(_) + f'F{contador_nomenclatura_familia(_)}' + contador_nomenclatura_hogar(_),
        'fecha': pd.to_datetime(row['fecha']).strftime('%Y-%m-%d') if pd.notna(row['fecha']) else '',
        'tipo_vivienda': convertidor_vivienda(row['vivienda']),
        'tipo_vivienda_desc':'',
        'material':convertidor_material(row['pared'],'8'),
        'material_desc': '',
        'piso':convertidor_material(row['piso'],'6'),
        'piso_desc':'',
        'techo':convertidor_material(row['techo'], '8'),
        'techo_desc':'',
        'numero_dormitorios': row['dormitorios'] if pd.notna(row['dormitorios']) and str(row['dormitorios']).isdigit() and int(row['dormitorios']) >= 0 else '0',
        'hacinamiento':convertidor_material(row['hacinamiento'], '2'),
        'riesgo_vivienda': convertidor_material(row['riesgo'], '11'),
        'acceso_vivienda': convertidor_material(row['acceso'], '5'),
        'combustible': convertidor_material(row['combustible'], '8'),
        'vector': convertidor_material(row['vector'], '2'),
        'riesgo_externo' : convertidor_material(row['riesgoexterno'], '19'),
        'riesgo_externo_desc': '',
        'actividad_economica': convertidor_material(row['actividad'], '2'),
        'mascotas': convertir_animales(row['mascotas']),
        'total_mascotas': contar_animales(convertir_animales(row['mascotas']), row['numeroPerros'], row['numeroGatos']),
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
        'numero_personas_familia': row['numeropersonas'] if pd.notna(row['numeropersonas']) and str(row['numeropersonas']).isdigit() and int(row['numeropersonas']) > 0 else '1',
        'resultado_familiograma': convertidor_calculo_familiograma(row['resultadofamiliograma'], '3'),
        'calculo_apgar': calculo_apgar(row['calculoapgar']),
        'cuidador': propiedades[5],
        'calculozarit': calculo_zarit(row['calculozarit']),
        'codigo_ecomapa': convertidor_material(row['resultadoecomapa'], '1'),
        'ninos_ninas': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'familia con niñas, niños y adolescentes'),
        'gestantes': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'gestantes'),
        'adultos_mayores': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'adultosmayores'),
        'victimas_conflicto': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'victima conflicto'),
        'poblacion_discapacidad': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'discapacidad'),
        'enfermedad_catastrofica': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'personas con enferemedades cronicas'),
        'enfermedad_trasmisible': '',
        'covivientes': '2',
        'familia_vulnerable': convertidor_poblacion_vulnerable(row['poblacionvulnerable'], 'no'),
        'estilo_vida': convertidor_material(row['estilodevidapredominante'], '2'),
        'antecedente_enfermedad': convertidor_material(row['antecedenteenfermedad'], '2'),
        'antecedente_enfermedad_desc': '',
        'alimentos': row['alimentos'] if pd.notna(row['alimentos']) and row['alimentos'] != '' else '9',
        'alimentos_desc': '',
        'estilodevidapredominante': convertidor_material(row['estilodevidapredominante'], '2'),
        'recursos_potenciadores':'2',
        'cuidado_entornos': '2',
        'practicas_relaciones_sanas':calculo_variables_segun_zarit(row['calculozarit']),
        'redes_colectivas':convertidor_poblacion_vulnerable(row['programasocial'], 'no'),
        'autonomia_adulto_mayor':'1',
        'prevencion_higiene': convertidor_poblacion_vulnerable(row['programasocial'], 'si'),
        'saberes_ancestrales':'2',
        'derecho_Salud': '1',
        'id_familia': propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}H' + contador_nomenclatura_familia(_) + propiedades[1] + propiedades[2] + propiedades[3] + row['territorio'] + row['microterritorio'].replace('0', 'MT', 1) + 'EBS' +  f'{1:03}H' + contador_nomenclatura_familia(_) + f'F{contador_nomenclatura_familia(_)}'
    } for _, row in df_info_general.iterrows()
    ])

    #
    df_responsables_fantasma = formato[formato['numero_documento_responsable'].isin(['', '0', None])]

    # Filtrar registros con NaN en longitud y latitud
    df_invalidos = formato[formato[['longitud', 'latitud']].isna().any(axis=1)]

    # Filtrar registros válidos (sin NaN en longitud y latitud)
    formato = formato.dropna(subset=['longitud', 'latitud'])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()

    return formato , df_invalidos, df_responsables_fantasma

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

def definir_pregunta_dos_opciones(param,esperada):
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

def evaluacion_poblacional(param , edad = None ,discapacidad = None, gestante = None,riesgo_psicosocial = None, sospecha_victima = None):

    param = limpiar_tildes(param).upper()
    discapacidad = limpiar_tildes(discapacidad).upper() if discapacidad else ''
    gestante = limpiar_tildes(gestante).upper() if gestante else ''
    edad = int(edad.split('.')[0])
    riesgo_psicosocial = limpiar_tildes(riesgo_psicosocial).upper()
    sospecha_victima = limpiar_tildes(sospecha_victima).upper()

    print(sospecha_victima)

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

    if riesgo_psicosocial not in opciones or sospecha_victima not in  opciones:
        return '6'

    if param == 'ADULTEZ' or param == 'JUVENTUD' or param == 'NO APLICA':
       return '7'

    return '7'

def limpiar_formato_tala(param):
    # con expresion regular quitar letras tildes comas puntos
    try:
        if pd.isna(param):
            return ''
        resultado = re.sub(r'[a-zA-Z.,\s]', '', param)

        if len(resultado) <= 2:
            param = resultado + '0'
        return resultado
    except ValueError:
        return ''

def limpiar_formato_peso(param):
    try:
        if pd.isna(param):
            return ''
        resultado = re.sub(r'[a-zA-Z.\s]', '', param)

        if len(resultado) == 3:
            return f"{int(resultado) / 10:.1f}"
        elif len(resultado) <= 2:
            return resultado + '.0'
        return resultado
    except ValueError:
        return ''


def enfermedades_cronicas(enfermedades):
    enfermedades_sin_tildes = {}
    for i in enfermedades:

        enfermedades_sin_tildes = {limpiar_tildes(i) :'1'}

    return enfermedades_sin_tildes

def registros_tipo_3(tipo_registro, df_personas, df_familias=None,valor_rango=1):

    # quitar los None
    df_personas = df_personas.replace('None', np.nan)

    # quitar none
    df_personas = df_personas.dropna(subset=['familia_id'])

    df_merged = pd.merge(
        df_personas,
        df_familias[['id_familia_db', 'numero_id_hogar', 'numero_id_familia']],
        left_on='familia_id',
        right_on='id_familia_db',
        how='left',  # 👈 Trae todas las personas aunque no tengan familia
        validate="many_to_one"  # Cada persona pertenece a una sola familia
    )


    formato = pd.DataFrame([{
        'id_familia_db': row['id_familia_db'],
        'tipo_registro': tipo_registro,
        'primer_nombre': limpiar_tildes(row['primer_nombre']) if pd.notna(row['primer_nombre']) and row['primer_nombre'] != '' else '',
        'segundo_nombre': limpiar_tildes(row['segundo_nombre']) if pd.notna(row['segundo_nombre']) and row['segundo_nombre'] != '' else '',
        'primer_apellido': limpiar_tildes(row['primer_apellido']) if pd.notna(row['primer_apellido']) and row['primer_apellido'] != '' else '',
        'segundo_apellido': limpiar_tildes(row['segundo_apellido']) if pd.notna(row['segundo_apellido']) and row['segundo_apellido'] != '' else '',
        'tipo_documento': convertidor_tipo_cedulas(row['tipodoc']),
        'numero_documento': row['numerodoc'].replace(',', '').strip(),
        'fecha_nacimiento': pd.to_datetime(row['fechanac']).strftime('%Y-%m-%d') if pd.notna(row['fechanac']) else '',
        'sexo': covertir_sexo(row['sexo']),
        'gestante': definir_pregunta_dos_opciones(row['gestacion'], 'SI'),
        'rol': convertidor_multicampos(row['rol'],['1','2','3','4','5','6']),
        'ocupacion':convertidor_material(row['ocupacion'], '9998'),
        'estudio': convertidor_multicampos(row['estudio'],NIVEL_ESTUDIO,'3'),
        'regimen_afiliacion': convertidor_multicampos(limpiar_tildes(row['regimen']),AFILIACION  , '5'),
        'eapb':'',
        'grupo_poblacional': evaluacion_poblacional(row['cursovida'],row['edad'],row['discapacidad'], row['riesgopsicosocial'], row['sopechamaltrato']),
        'etnia': convertidor_multicampos(row['etnia'],ETNIA , '07'),
        'medico_tradicional':'',
        'pueblo_indigena':'',
        'condiciones_disca': convertidor_multicampos(limpiar_tildes(row['discapacidad']),DISCAPACIDAD, '8'),
        'peso': limpiar_formato_peso(row['peso']),
        'talla': limpiar_formato_tala(row['talla']),
        'peso_talla': '',
        'perimetro_branquial': '',
        'codiciones_cronicas': convertidor_multicampos(row['condicioncronica'], enfermedades_cronicas(ENFERMEDADES_CRONICAS), '2'),
        'materno': '',
        'intervenciones_canalizaciones': '',
        'motivos_no_atencion':'',
        'practica_deporte':'',
        'lactancia_materna' : '',
        'memses_lactancia':'',
        'signos_desnutricion':'',
        'gastrica_o_respiratoria':'',
        'gastrica_o_respiratoria_desc':'',
        'atencion_enfermedad_aguda':'',
        'motivos':'',
        'numero_id_hogar': row['numero_id_hogar'] + row['numero_id_familia'],
        'id_integrante': row['numero_id_hogar'] + row['numero_id_familia'] + convertidor_tipo_cedulas(row['tipodoc']) + row['numerodoc'],
    } for _, row in df_merged.iterrows()
    ])

    formato = formato.drop_duplicates(subset=['numero_documento'])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()


    # ordenar por id_familia_db
    formato = formato.sort_values(by=['id_familia_db']).reset_index(drop=True)

        # agregar consecutivo de registro
    formato.insert(2, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))

    # Check the columns in df_info_general
    return formato

def main():
    numero = limpiar_formato_longitud('-77.26670')
    numero_lat = limpiar_formato_latitud('1.201450')

    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2024,
        autocommit=False  # Disable autocommit
    )

    try:
        cursor = connection.cursor()
        familias_query = ejecutar_consulta_mysql(query_familias(TERRITORIO, MICROTERRITORIO) , cursor)

        if not familias_query:
            print("No se encontraron familias para el territorio y microterritorio especificados.")
            return

        FE_REPORTE= datetime.now().strftime('%Y-%m-%d')

        df_familias = pd.DataFrame(familias_query, columns=[
            'id_familia_db',
            'id_sociambiental_db',
            'latitud',
            'longitud',
            'direccion',
            'hacinamiento',
            'territorio',
            'microterritorio',
            'nombre_barrio',
            'estrato',
            'numerohogares',
            'numerohabitantes',
            'hogar',
            'tipodocr',
            'docr',
            'profesion',
            'fecha',
            'vivienda',
            'pared',
            'piso',
            'techo',
            'dormitorios',
            'riesgo',
            'acceso',
            'combustible',
            'vector',
            'riesgoexterno',
            'actividad',
            'mascotas',
            'numeroPerros',
            'numeroGatos',
            'aguaservicio',
            'diposicionexcretas',
            'aguaresiduales',
            'basura',
            'tipofamilia',
            'numeropersonas',
            'resultadofamiliograma',
            'calculoapgar',
            'calculozarit',
            'zaritfuncionalidad',
            'resultadoecomapa',
            'poblacionvulnerable',
            'riesgopsicosocial',
            'estilodevidapredominante',
            'antecedenteenfermedad',
            'antecedenteenfermedad1',
            'antecedenteenfermedad2',
            'saludalternativa',
            'alimentos',
            'programasocial',
            'higiene'
        ])

        # Crear un gráfico de torta para la columna 'hacinamiento'

        postulados_tipo_2, falla_cordenadas , responsables_malos = registro_tipo_2(TIPO_REGISTROS[1], PROPIEDADES_TIPO_2, df_familias.drop_duplicates(subset=['id_familia_db']))

        id_list = postulados_tipo_2['id_familia_db'].tolist()
        id_list_sql = ', '.join(map(str, id_list))  # Convert to a string for SQL
        query_personas_adultas = ejecutar_consulta_mysql(traer_joven_adultos(id_list_sql), cursor)
        df_personas = pd.DataFrame( query_personas_adultas, columns= COLUMNAS_PERSONAS_JOVENADULTO)  # DataFrame vacío para personas, ya que no se usa en este ejemplo

        df_familias_a_crear = postulados_tipo_2[postulados_tipo_2['id_familia_db'].isin(df_personas['familia_id'].unique())]
        # agregar consecutivo de registro
        df_familias_a_crear.insert(2, 'consecutivo_registro', range(1, len(df_familias_a_crear) + 1))

        df_familias_sin_personas = df_familias[~df_familias['id_familia_db'].isin(df_personas['familia_id'].unique())]

        tipo_3 = registros_tipo_3(TIPO_REGISTROS[2], df_personas, df_familias_a_crear, len(df_familias_a_crear) + 1 )
        tipo_1 = registro_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, FECHA_INICIAL, FECHA_FINAL, len(tipo_3) + len(df_familias_a_crear))


        os.makedirs(F'reportes/{FE_REPORTE}', exist_ok=True)
        falla_cordenadas.to_csv(F'reportes/{FE_REPORTE}/falla_cordenadas_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')
        responsables_malos.to_csv(F'reportes/{FE_REPORTE}/responsables_malos_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')
        df_familias_sin_personas.to_csv(F'reportes/{FE_REPORTE}/familias_sin_personas_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv')

        reporte = pd.concat([tipo_1, df_familias_a_crear, tipo_3], ignore_index=True)
        reporte.to_csv(f'reportes/{FE_REPORTE}/reporte_aps124_{FE_REPORTE}_{TERRITORIO}_{MICROTERRITORIO}.csv', index=False)

        tipo_3 = tipo_3.iloc[:, 1:]  # Eliminar la primera columna
        tipo_2 = df_familias_a_crear.iloc[:, 1:]

        consolidado = codificar_formato(tipo_1) + '\n'
        consolidado += codificar_formato(tipo_2) + '\n'
        consolidado += codificar_formato(tipo_3)

        file_name = f"reportes/{FE_REPORTE}/APS124CCFP{FECHA_INICIAL.replace('-','')}NI000900091143.txt"



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