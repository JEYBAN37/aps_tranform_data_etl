from datetime import datetime
from io import BytesIO
import pandas as pd


def main():

    PROPIEDADES_TIPO_1 = [
    # 1
    'NI',
    # 2
    '900091143',
    # 5,
    'EMPRESA SOCIAL DE ESTADO PASTO SALUD ESE',
    # 6
    '900091143',
    # 7
    'CC',
    # 8
    '79453251',
    # 9
    'DIEGO',
    # 10
    'MORALES',
    # 11
    'CC',
    # 12
    '93407260',
    # 13
    'DIEGO',
    # 14
    'MORALES',
    # 15
    'GERENCIA@PASTOSALUDESE.GOV.CO'
    ]

    # 0
    TIPO_REGISTROS = ['1','2', '3']
    # 3
    FE_CORTE = datetime.now().strftime('%Y-%m-%d')

    # Registros de usuarios
    # Cargar CV

    # URL of the Excel file
    url = 'https://docs.google.com/spreadsheets/d/1RqbfsgJc9N-qOJfmveLQTm1GNwwbO-0S/export?format=xlsx&gid=749204967'

    # Read the Excel file
    df = pd.read_excel(url, engine='openpyxl')  # Ensure `openpyxl` is installed

    # Ensure all string columns are processed to handle special characters
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.normalize('NFKD')

    df = df[
        df['N° CEDULA'].notnull() & (df['N° CEDULA'] != '')
        ].copy()

    print(df['N° CEDULA'])

    CEDULAS_NO_REPORTADAS = [
        "1004131976", "1004190710", "1004190984", "1004235479", "1004564013",
        "1022426162", "1081052746", "1081052779", "1081053045", "1081053820",
        "1081272318", "1082747438", "1085247449", "1085252263", "1085260497",
        "1085263018", "1085284997", "1085286725", "1085310606", "1085315209",
        "1085318201", "1085323163", "1085331301", "1085333467", "1085336362",
        "1085340885", "1086016304", "1086361622", "1087413951", "1087414586",
        "1087645341", "1087960258", "1193035720", "1233190311", "1233190640",
        "1233192708", "27549931", "36758143", "5930737", "59312865", "59313415",
        "59816986", "59824313", "59825948"
    ]

    # crea dtaframe

    df_no_reportados = pd.DataFrame(CEDULAS_NO_REPORTADAS, columns=['N° CEDULA'])
    df_no_inscirtos = df[df['N° CEDULA'].astype(str).isin(df_no_reportados['N° CEDULA'])].copy()

    # df = df[~df['N° CEDULA'].astype(str).isin(df_no_reportados['N° CEDULA'])].copy()

    tipo_2 = registros_tipo_2(TIPO_REGISTROS[1], df)
    tipo_3 = registros_tipo_3(TIPO_REGISTROS[2], df , len(tipo_2))
    tipo_1 = registro_tipo_1(TIPO_REGISTROS[0], PROPIEDADES_TIPO_1, FE_CORTE, len(tipo_2) + len(tipo_3))

    consolidado = codificar_formato(tipo_1) + '\n'
    consolidado += codificar_formato(tipo_2) + '\n'
    consolidado += codificar_formato(tipo_3)

    file_name = f"txt/SEG500USIN{FE_CORTE.replace('-','')}NI000900091143.txt"

    df_no_inscirtos.to_csv('reportes/usuarios_no_reportados.csv')

    # Guardar el archivo en la misma carpeta
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(consolidado)


def registro_tipo_1(tipo_registro, propiedades, fecha_corte, num_total_registros):
    formato = pd.DataFrame([{
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': propiedades[0],
        'numero_identificacion': propiedades[1],
        'fecha_corte': fecha_corte,
        'numero_total_registros': num_total_registros,
        'razon_social': propiedades[2],
        'nit': propiedades[3],
        'tipo_identificacion_rp': propiedades[4],
        'numero_identificacion_rp': propiedades[5],
        'nombre_rp': propiedades[6],
        'apellido_rp': propiedades[7],
        'tipo_identificacion_ct': propiedades[8],
        'numero_identificacion_ct': propiedades[9],
        'nombre_ct': propiedades[10],
        'apellido_ct': propiedades[11],
        'correo_electronico_entidad': propiedades[12]
    }])

    # poenr mayusculas a todos los campos de tipo string
    for col in formato.select_dtypes(include=['object']).columns:
        formato[col] = formato[col].str.upper()

    return formato

def extraer_primer_apellido(nombre_completo):
    """
    Extracts the first last name from a full name, considering connectors.
    """
    conectores = {'de', 'del', 'la', 'las', 'los', 'y', 'mc', 'mac', 'van', 'von', 'es'}
    palabras = nombre_completo.strip().split()

    # Filter out connectors
    palabras_filtradas = [p for p in palabras if p.lower() not in conectores]

    # If there are exactly three words, return the second word
    if len(palabras_filtradas) == 3:
        return palabras_filtradas[1]

    # Return the last word as the first last name
    if palabras_filtradas:
        return palabras_filtradas[-1]
    return ''  # Return empty if no valid words are found


def registros_tipo_2(tipo_registro, df_usuarios):
    fuente = df_usuarios.copy()

    limpiar_nombre = fuente['NOMBRE'].astype(str).str.replace(r'[^a-zA-Z\s]', '', regex=True)

    # limpiar_nombre = fuente['NOMBRE'].astype(str).str.replace(r'[^a-zA-ZñÑ\s]', '', regex=True)

    formato = pd.DataFrame({
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': 'CC',
        'numero_identificacion': fuente['N° CEDULA'].astype(str).str.split('.').str[0],
        'primer_nombre': limpiar_nombre.str.split().str[0].str.upper(),
        'primer_apellido': limpiar_nombre.apply(extraer_primer_apellido).str.upper(),
        'indicador_vinculacion': 'V',
        'telefono': fuente['TELEFONO'].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:10],
        'cargo': fuente['PERFIL'].astype(str).str.upper(),
        'contrato': '0',
        'fin':'2025-12-31',
        'correo_electronico': 'pastosaludeseaps@gmail.com'
    })

    formato.insert(1, 'consecutivo_registro', range(1, len(formato) + 1))

    return formato

def registros_tipo_3(tipo_registro, df_usuarios , inicio_consecutivo):
    fuente = df_usuarios.copy()

    formato = pd.DataFrame({
        'tipo_registro': tipo_registro,
        'tipo_identificacion_identidad': 'CC',
        'numero_identificacion': fuente['N° CEDULA'].astype(str).str.split('.').str[0],
        'aplicacion': '186',
        'indicador': 'A',
        'perfil': '1274',
    })
    valor_rango = inicio_consecutivo + 1
    formato.insert(1, 'consecutivo_registro', range(valor_rango, valor_rango + len(formato)))
    return formato


def codificar_formato(df, separador='|', nuevo_separador='|', header=False, index=False):
    # Rango del indice apartir de 1 si index es True
    if index:
        df.index = range(1, len(df) + 1)

    # Crear el archivo en memoria
    bufferf = BytesIO()

    # Exportar el DataFrame a un archivo .txt en memoria
    df.to_csv(bufferf, sep=separador, header=header, index=index)

    # Leer y modificar el contenido en memoria
    bufferf.seek(0)  # Volver al inicio del archivo en memoria
    data = bufferf.read().decode('utf-8')  # Leer y decodificar el contenido en texto

    # Reemplazar el separador temporal por el separador final
    # Reemplazar el separador temporal por el separador final y eliminar espacios al final de cada fila
    data = data.replace(separador, nuevo_separador).strip()

    # Eliminar líneas en blanco al final de la data
    data = '\n'.join(line.rstrip() for line in data.splitlines() if line.strip())

    return data

if __name__ == "__main__":
    main()