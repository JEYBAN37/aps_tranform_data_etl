import re

import pandas as pd
from fontTools.tfmLib import MATHSY

if __name__ == "__main__":

    # Matriz de microterritorios
    url = "bases_downloads/produccion_filtrada_2026_EBS_microterritorios.csv"
    matriz_microterritorios_LIMPIA = pd.read_csv(url, encoding='latin1')
    poblacion_vulnerable = pd.read_csv("bases_downloads/poblacion_vulnerable_x_microterritorio.csv", encoding='latin1',sep ='|')
    micro_proyectados = pd.read_excel("bases_downloads/microterritorio_proyectado.xlsx")

    equipos_territorio = pd.read_excel(r"C:\Users\Esteban Getial\OneDrive\escaneos\EBS-EQUIPOS.xlsx")

    # limpiar microterritorio
    poblacion_vulnerable['microterritorio'] = poblacion_vulnerable['microterritorio'].str.extract(r'(MT\S*)')

    # limpiar microterriotiro hay territorios que tienie t1 a t9 cambialos a t09 y asi
    matriz_microterritorios = matriz_microterritorios_LIMPIA[~matriz_microterritorios_LIMPIA['territorio'].isin(['SIN_COBERTURA_BARRIO', 'SIN_COBERTURA_GEOJSON','SIN_DATOS'])]

    matriz_microterritorios['microterritorio'] = matriz_microterritorios['microterritorio'].str.extract(r'(MT\S*)')
    matriz_microterritorios['microterritorio'] = matriz_microterritorios['microterritorio'].str.replace('0', '', regex=True)

    matriz_microterritorios['territorio'] = matriz_microterritorios['territorio'].str.replace(r'T([1-9])$', r'T0\1', regex=True)
    # Ordenar por territorio y microterritorio
    matriz_microterritorios = matriz_microterritorios.sort_values(by=['territorio', 'microterritorio'])

    numero_primera_infancia = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 0) & (matriz_microterritorios['Edad'] <= 5)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_niños_primera_infancia')
    numero_valoracion_integral_primera_infancia = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 0) & (matriz_microterritorios['Edad'] <= 5) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_niños_valoracion_integral_primera_infancia')

    numero_infancia = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 6) & (matriz_microterritorios['Edad'] <= 11)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_niños_infancia')
    numero_valoracion_integral_infancia = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 6) & (matriz_microterritorios['Edad'] <= 11) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_niños_valoracion_integral_infancia')


    numero_adolescentes = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 12) & (matriz_microterritorios['Edad'] <= 17)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adolescentes')
    numero_valoracion_integral_adolescentes = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 12) & (matriz_microterritorios['Edad'] <= 17) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adolescentes_valoracion_integral')

    numero_juventud = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 18) & (matriz_microterritorios['Edad'] <= 28)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_juventud')
    # VALORACIÓN INTEGRAL JUVENTUD (18-28)
    numero_valoracion_integral_juventud = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 18) & (matriz_microterritorios['Edad'] <= 28) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_juventud_valoracion_integral')

    # VALORACIÓN INTEGRAL JUVENTUD (24-28)
    numero_valoracion_integral_juventud_24_28 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 24) & (matriz_microterritorios['Edad'] <= 28) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_juventud_valoracion_integral_24_28')

    # VALORACIÓN INTEGRAL JUVENTUD (18-23)
    numero_valoracion_integral_juventud_18_23 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 18) & (matriz_microterritorios['Edad'] <= 23) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_juventud_valoracion_integral_18_23')

    numero_adultes = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 29) & (matriz_microterritorios['Edad'] <= 59)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adultos')

    numero_adultes_valoracion_integral_29_44 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 29) & (matriz_microterritorios['Edad'] <= 44) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adultos_valoracion_integral_29_44')
    numero_adultes_valoracion_integral_45_59 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 45) & (matriz_microterritorios['Edad'] <= 59) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adultos_valoracion_integral_45_59')
    numero_adultes_valoracion_integral_29_59 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 29) & (matriz_microterritorios['Edad'] <= 59) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_adultos_valoracion_integral_29_59')

    numero_vejez = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 60)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_vejez')
    numero_valoracion_integral_vejez = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 60) & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS','CONSULTA DE PRIMERA VEZ POR ENFERMERIA RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_valoracion_integral_vejez')

    numero_mujeres_28_66 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 28) & (matriz_microterritorios['Edad'] <= 66) & (matriz_microterritorios['Sexo'] == 'F')].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_mujeres_28_66')
    numero_mujeres_tamizaje_cuello = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 25) & (matriz_microterritorios['Edad'] <= 66) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['Nombre_Servicio'].isin(['TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_mujeres_tamizaje_cuello')

    numero_mujeres_52_70 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 52) & (matriz_microterritorios['Edad'] <= 70) & (matriz_microterritorios['Sexo'] == 'F')].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_mujeres_52_70')
    numero_mujeres_tamizaje_mamografia = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 50) & (matriz_microterritorios['Edad'] <= 69) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE CONTROL MEDICO GENERAL RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_mujeres_tamizaje_mamografia')

    numero_52_76 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 52) & (matriz_microterritorios['Edad'] <= 76)].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_52_76')

    numero_hombres_55_76 = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 55) & (matriz_microterritorios['Edad'] <= 76) & (matriz_microterritorios['Sexo'] == 'M')].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_hombres_55_76')
    numero_tamizaje_prostata = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 50) & (matriz_microterritorios['Edad'] <= 75) & (matriz_microterritorios['Sexo'] == 'M') & (matriz_microterritorios['Nombre_Servicio'].isin(['ANTIGENO ESPECIFICO DE PROSTATA SEMIAUTOMATIZADO O AUTOMATIZADO']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_tamizaje_prostata')

    conteo_familias = matriz_microterritorios.drop_duplicates('familia_id').groupby(['territorio', 'microterritorio']).size().reset_index(name='conteo_familias')
    conteon_personas = matriz_microterritorios.drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='conteo_personas')

    numero_consulta_primera_vez_prenatal = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 10) & (matriz_microterritorios['Finalidad'] == '23') & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['programa'] == 'RUTA MATERNO PERINATAL') & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE CONTROL MEDICO GENERAL RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_primera_vez_prenatal')
    numero_a_consejeria_vih_y_sifilis = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 10) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['programa'] == 'RUTA MATERNO PERINATAL') & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSEJERIA DE VIH RIAS','CONSEJERIA DE VIH POR MEDICINA GENERAL','CONSEJERIA DE VIH','PRUEBA RAPIDA TREPONEMA PALLIDUM']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_consejeria_vih_sifilis')
    numero_vih = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 10) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['programa'] == 'RUTA MATERNO PERINATAL') & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSEJERIA DE VIH RIAS','CONSEJERIA DE VIH POR MEDICINA GENERAL','CONSEJERIA DE VIH']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_vih')
    numero_sifilis = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 10) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['programa'] == 'RUTA MATERNO PERINATAL') & (matriz_microterritorios['Nombre_Servicio'].isin(['PRUEBA RAPIDA TREPONEMA PALLIDUM']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_sifilis')
    numero_preocncepcional = matriz_microterritorios[(matriz_microterritorios['Edad'] >= 10) & (matriz_microterritorios['Sexo'] == 'F') & (matriz_microterritorios['programa'] == 'RUTA MATERNO PERINATAL') & (matriz_microterritorios['Nombre_Servicio'].isin(['CONSULTA DE CONTROL MEDICO GENERAL RIAS']))].drop_duplicates('Identificacion').groupby(['territorio', 'microterritorio']).size().reset_index(name='numero_preconcepcional')

    matriz = pd.merge(
        conteo_familias,
        conteon_personas, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_primera_infancia, on=['territorio', 'microterritorio'], how='outer')


    matriz = pd.merge(
        matriz, numero_valoracion_integral_primera_infancia, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_infancia, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_infancia, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_adolescentes, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_adolescentes, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_juventud, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_juventud, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_juventud_24_28, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_juventud_18_23, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_adultes, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_adultes_valoracion_integral_29_44, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_adultes_valoracion_integral_45_59, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_adultes_valoracion_integral_29_59, on=['territorio', 'microterritorio'], how='outer')


    matriz = pd.merge(
        matriz, numero_vejez, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_valoracion_integral_vejez, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_mujeres_28_66, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_mujeres_tamizaje_cuello, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_mujeres_52_70, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_mujeres_tamizaje_mamografia, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_52_76, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_hombres_55_76, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_tamizaje_prostata, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_consulta_primera_vez_prenatal, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_a_consejeria_vih_y_sifilis, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_vih, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_sifilis, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, numero_preocncepcional, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, poblacion_vulnerable, on=['territorio', 'microterritorio'], how='outer')

    matriz = pd.merge(
        matriz, micro_proyectados, on=['territorio', 'microterritorio'], how='outer')

    # 1. Crear el detalle combinando perfil y nombre (como ya lo hiciste)
    equipos_territorio['detalle'] = equipos_territorio['PERFIL'] + ' : ' + equipos_territorio['NOMBRE']

    # 2. Agrupar por 'EBS' (territorio) y concatenar todos los profesionales en una sola fila por territorio
    ebs_agrupado = equipos_territorio.groupby('EBS')['detalle'].agg(', '.join).reset_index()

    # 3. Renombrar la columna resultante a 'grupo_ebs'
    ebs_agrupado.rename(columns={'detalle': 'grupo_ebs'}, inplace=True)

    # 4. Asegurar que los tipos de datos de la llave de cruce coincidan (texto plano)
    matriz['territorio'] = matriz['territorio'].astype(str).str.strip()
    ebs_agrupado['EBS'] = ebs_agrupado['EBS'].astype(str).str.strip()

    # 5. Hacer el merge final con la matriz
    matriz = pd.merge(
        matriz,
        ebs_agrupado,
        left_on='territorio',
        right_on='EBS',
        how='outer'
    )

    matriz = matriz.sort_values(by=['territorio','microterritorio'])


    print(matriz_microterritorios.head())

    matriz.to_csv("bases_downloads/matriz_microterritorios.csv", index=False)