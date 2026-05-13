import os

import pandas as pd

from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025, URL_CONTRATACION_PLANTILLA
import mysql.connector

from limpieza_datos import extraer_distribucion_redes


def procesar_base():

    url ="bases_downloads/Informe de Produccion Servicio Plan Unidad (13).xlsx"
    produccion = pd.read_excel(url, skiprows=2)
    # Luego eliminamos las primeras 2 columnas si no las necesitas
    df_produccion = produccion.iloc[:, 2:]
    print(df_produccion.head())

    df_responsables,df_personas = df_cruce_con_db()

    df_responsables_activo = df_responsables[df_responsables['contrato'] == 'ACTIVO']

    df_produccion_filtrada = df_produccion[df_produccion['Ident Medico'].isin(df_responsables_activo['numero'])]

    df_produccion_filtrada.drop_duplicates(subset=['Identificacion'], inplace=True)

    df_hogares_atendidos = df_produccion_filtrada[df_produccion_filtrada['Identificacion'].isin(df_personas['numero'])]





    # PRIMERO APLICAR FILTRO NUM PLAN SOLO PARA ENSSANAR PGPCRONICOS ADICIONAL CRONICOS





    # SUBSIADIADO CAPUTADO  CAPITA RIAS APLICAR LOS FILTROS ADICIONAL FILTROS SERVICIOS AMBULATORIO PREVENTICON

    PERFILES = [
        ('JEFE DE ENFERMERIA'),
        ('AUXILIAR DE ENFERMERIA'),
        'MEDICINA GENERAL0,'
        'PSICOLOGIA'
        'NUTRICION CLINICA',
        'ODONTOLOGIA',
        'GINECOLOGIA Y OBSTETRICIA',
        'MEDICINA INTERNA',
        'PEDIATRIA'
    ]

    # campo nombre de servicio
    SERVICIOS = ["CONSULTA DE CONTROL O SEGUIMIENTO POR ENFERMERA",
                 "CONSULTA DE CONTROL O SEGUIMINETO POR ENFERMERIA RIAS","CONSULTA POR PRIMERA VEZ POR ENFERMERIA RIAS",
                 "TAMIZAJE POR RIESGO CARDIOVASCULAR",
                 "ATENCION VISITA DOMICILIARIA POR ENFERMERIA"]


    # FILTRO LA HOJA DE RIAS EDAD
    # PRIMERA INFANCIA 0.08 - 6 AÑOS
    # INFANCIA 6 AÑOS 12
    # ADOLESCENCIA 12 AÑOS 18
    # JUVENTUD 18 AÑOS 29
    # ADULTEZ 29 AÑOS 60
    # VEJEZ 60 X 100PRE

    # RUTA MATERNO SOLO APLICA MUJERES 11 AÑOS 50 SI EL DAINOSTICO EMPIEZZA POR DIAGNOSTICO PRINCIPAL SIEMPRE DEBE SER "Z"
    # Z321
    # Z713
    # Z300
    # Z391
    # Z358
    # Z392
    # Z392

    # FILTRRAR CRONICOS EN NUM PLAM =  pgp

    # DUPLICADO QUITAR POR CEDULA MISMO NOMBRE DE SERVICIO SI TIENEN EL MISMO

    print(f"Hay {len(df_hogares_atendidos)} filas")
    print(f"Hay {len(df_produccion_filtrada)} filas")

def df_cruce_con_db():
    global cursor


    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2025,
        autocommit=False  # Disable autocommit
    )

    try:

        url = "bases_downloads/reporte_unificado_2024_2026.csv"
        produccion = pd.read_csv(url)
        # Luego eliminamos las primeras 2 columnas si no las necesitas
        print(produccion.head())

        cursor = connection.cursor()
        responsables_ebs = cargar_responsables( cursor, DATABASE)
        connection.commit()

        df_contratacion = pd.read_csv(URL_CONTRATACION_PLANTILLA)

        df_contratacion['identificacion_contratista'] = df_contratacion['identificacion_contratista'].astype(str).str.strip().str.split(".").str[0]

        produccion['Ident Medico'] = produccion['Ident Medico'].astype(str).str.strip().str.split(".").str[0]
        produccion['CodPres'] = produccion['CodPres'].astype(str).str.strip().str.split(".").str[0]

        df_produccion_cruzada = produccion[produccion['Ident Medico'].isin(df_contratacion['identificacion_contratista']) | produccion['CodPres'].isin(df_contratacion['identificacion_contratista'])]


        df_aps_atenciones = pd.DataFrame([{
                'identificacion_paciente': row['Identificacion'],
                'nombre_paciente': row['Nombre Paciente'] | row['Nombre_Paciente'],
                'fecha_nacimiento': row['Fecha Nac'] | row['FechaNac'],
                'telefono': row['TelRes'] | row['Telefono'],
                ''
                'direccion_paciente': row['DirAfil'] | row['Dir Afil'],
                'identificacion_medico': row['Ident Medico'] | row['CodPres'],
                'nombre_medico': row['Nombre'] | row['Nombre Medico'],

            } for _, row in df_produccion_cruzada.iterrows()
            ])


        df_produccion_cruzada = df_produccion_cruzada.drop_duplicates(subset=['Identificacion', 'Ident Medico'], keep='first')

        grupos = df_contratacion.groupby(by=['resolucion'])
        periodos_contratacion = [grupo for _, grupo in grupos]
        for resolucion in periodos_contratacion:

            # hacer una fecha inicial y final
            # con entre la fecha de contratao ams reciente y l afecha de finalizacion mas antigua y segun eso tomar las fechas

            fecha_fin = resolucion['']

            atenciones = produccion

            caracaterizaciones = "verificar varibles de las personas"





        df_personal_total = df_contratacion.merge()

        df_resolucion_actual = responsables_ebs[responsables_ebs['contrato'] == 'ACTIVO']


        cursor = connection.cursor()
        df_distribucion_redes = extraer_distribucion_redes()
        personas = cargar_personas(cursor, df_distribucion_redes,'','client',DATABASE,"1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo")
        connection.commit()

        return responsables_ebs,personas



    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


def unir_reportes_facturacion():
    # una ruta abrir losa rchivo csv o xlsx y unirlos en un solo dataframe
    ruta = "E:\FACTURACION (1)\FACTURACION\APS"
    df_principal = pd.DataFrame()

    # mejor concatenación: leer todos los archivos, normalizar columna Identificacion y concatenar
    dfs = []
    for archivo in os.listdir(ruta):
        if archivo.lower().endswith((".csv", ".xlsx", ".xls")):
            path = os.path.join(ruta, archivo)
            try:
                if archivo.lower().endswith(".csv"):
                    df = pd.read_csv(path, skiprows=2, dtype=str,
                                     on_bad_lines='warn'
                                     )
                else:
                    df = pd.read_excel(path, skiprows=2, dtype=str)
            except Exception as e:
                print(f"Skipping {path}: {e}")
                continue
            df = df.iloc[:, 2:]
            dfs.append(df)
            print(f"Processed {archivo} with {len(df)} rows.")

    if not dfs:
        df_principal = pd.DataFrame()
    else:
        df_concat = pd.concat(dfs, ignore_index=True, sort=False)
        # conservar la primera aparición no nula por Identificacion
        df_principal = df_concat
    out_path = os.path.join("bases_downloads", "reporte_unificado_2024_2026.csv")
    df_principal.to_csv(out_path, index=False)

if __name__ == "__main__":
    #unir_reportes_facturacion()
    df_cruce_con_db()