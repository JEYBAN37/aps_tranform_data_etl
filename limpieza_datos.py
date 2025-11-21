import os

import mysql
import pandas as pd
from google.oauth2.service_account import Credentials
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, DATABASE_APS2024, MYSQL_REPLICA_PASSWORD, DATABASE
from export_aps_124 import limpiar_formato_latitud, limpiar_formato_longitud
from mysql_conector import ejecutar_consulta_mysql
from datetime import datetime
import gspread


def reescribir_hoja(sheet_id, sheet_name, df, client):
    df = df.fillna("NULL")
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    sheet.clear()
    # Ahora sí, convertir para Google Sheets
    data = [df.columns.tolist()] + df.values.tolist()
    # Convertir Timestamps a string
    sheet.update("A1", data)


def main():

    global cursor

    connection = mysql.connector.connect(
        host=MYSQL_APS,
        user=MYSQL_REPLICA_USER,
        password=MYSQL_REPLICA_PASSWORD,
        database=DATABASE_APS2024,
        autocommit=False  # Disable autocommit
    )

    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    creds = Credentials.from_service_account_file("credentials.json", scopes=scope)

    client = gspread.authorize(creds)

    try:

        cursor = connection.cursor()
        acumulado_personas = []
        acumulado_familias = []
        for database in DATABASE:
            personas_query = ejecutar_consulta_mysql(f"""SELECT 
                '{database}' AS db,
                t.doc_id,
                t.familia_id,
                t.tipodocumento ,
                t.primerapellido ,
                t.segundoapellido ,
                t.primernombre ,
                t.segundonombre ,
                t.gestacion ,
                t.condicioncronica,
                t.esquemavacunacion,
                t.desparasitacion ,
                t.valoracion,
                t.higiene_oral,
                t.aseguradora ,
                t.regimen ,
                t.metodosanticonceptivos,
                t.infeccionestransmisionsexual,
                t.controlP,
                t.consumospa,
                t.tomacitologia,
                t.mamografia,
                t.remisionespecifica,
                t.discapacidad,
                t.canalizacionuno , 
                t.canalizaciondos ,
                t.canalizaciontres ,
                s.id,
                t.cursodevida,
                s.fecha,
                s.apellidosfamilia,
                u.microterritorio,
                u.cod_microterritorio,
                u.comuna,
                u.territorio,
                u.zona,
                r.nombres,
                r.numero,
                r.ebs,
                CASE 
                    WHEN t.familia_id IS NULL THEN 'SIN_FAMILIA_ID'
                    WHEN f.id IS NULL THEN 'ID_FAMILIA_INVALIDO'
                    WHEN COUNT(*) OVER (PARTITION BY t.doc_id) = 1 THEN 'UNICO_Y_VALIDO'
                    ELSE 'DUPLICADO'
                END AS estado
            FROM (
                SELECT 
                numerodoc AS doc_id ,
                familia_id, 
                tipodocumento , 
                primerapellido , 
                segundoapellido , 
                primernombre , 
                segundonombre,
                gestacion ,
                condicioncronica,
                esquemavacunacion,
                desparasitacion ,
                valoracionmedica as valoracion,
                saludoral as higiene_oral,
                aseguradora ,
                regimen ,
                metodosanticonceptivos,
                infeccionestransmisionsexual,
                controlprenatal as controlP,
                consumospa,
                tomacitologia,
                mamografia,
                remisionespecifica,
                discapacidad,
                canalizacionuno ,
                canalizaciondos ,
                canalizaciontres ,
                'Adulto' as cursodevida
            	FROM {database}.juventudadultos
                UNION ALL
                SELECT 
                numerodoc AS doc_id ,
                familia_id, 
                tipodocumento , 
                primerapellido , 
                segundoapellido , 
                primernombre , 
                segundonombre,
                'NO APLICA' as gestacion ,
                condicioncronica,
                esquemavacunacion,
                desparasitacion ,
                crecimientoydesarrollo as valoracion,
                higieneoral  as higiene_oral,
                aseguradora ,
                regimen ,
                'NO APLICA' as  metodosanticonceptivos,
                 'NO APLICA' as infeccionestransmisionsexual,
                 'NO APLICA' as controlP,
                 'NO APLICA' as consumospa,
                 'NO APLICA' as tomacitologia,
                 'NO APLICA' as mamografia,
                remisionespecifica,
                discapacidad,
                canalizacionuno ,
                canalizaciondos ,
                canalizaciontres ,
                'Infante' as cursodevida FROM {database}.infantils
                UNION ALL
                SELECT 
                    numerodoc AS doc_id ,
                familia_id, 
                tipodocumento , 
                primerapellido , 
                segundoapellido , 
                primernombre , 
                segundonombre,
                'NO APLICA' as gestacion ,
                condicioncronica,
                esquemavacunacion,
                desparasitacion ,
                crecimientoydesarrollo as valoracion,
                higieneoral  as higiene_oral,
                aseguradora ,
                regimen ,
                'NO APLICA' as  metodosanticonceptivos,
                 'NO APLICA' as infeccionestransmisionsexual,
                 'NO APLICA' as controlP,
                 'NO APLICA' as consumospa,
                 'NO APLICA' as tomacitologia,
                 'NO APLICA' as mamografia,
                remisionespecifica,
                discapacidad,
                canalizacionuno ,
                canalizaciondos ,
                canalizaciontres ,
                'PrimeraInfancia' as cursodevida FROM {database}.primerainfancias
                UNION ALL
                SELECT     
                numerodoc AS doc_id ,
                familia_id, 
                tipodocumento , 
                primerapellido , 
                segundoapellido , 
                primernombre , 
                segundonombre,
                gestacion ,
                condicioncronica,
                esquemavacunacion,
                desparasitacion ,
                valoracionmedica as valoracion,
                saludoral as higiene_oral,
                aseguradora ,
                regimen ,
                metodosanticonceptivos,
                infeccionestransmisionsexual,
                controlprenatal as controlP,
                consumospa,
                'NO APLICA' as tomacitologia,
                'NO APLICA' as mamografia,
                remisionespecifica,
                discapacidad,
                canalizacionuno ,
                canalizaciondos ,
                canalizaciontres ,
                'Adolescencia' as cursodevida FROM {database}.adolescencias
            ) t
            LEFT JOIN {database}.familias f ON f.id = t.familia_id
            LEFT JOIN {database}.sociambientals s ON f.sociambiental_id = s.id 
            LEFT JOIN {database}.ubicaciones u ON s.ubicacion_id = u.id
            LEFT JOIN {database}.responsables r ON s.responsable_id  = r.id	
            """, cursor)
            if not personas_query:
                print(f"No se encontraron familias para {database}. Continuando.")
                continue
            acumulado_personas.extend(personas_query)
        connection.commit()

        personas = acumulado_personas
        df_personas_consolidados = pd.DataFrame(personas)
        df_personas_consolidados.columns = [desc[0] for desc in cursor.description]


        cursor = connection.cursor()
        for database in DATABASE:
            familias_query = ejecutar_consulta_mysql(f"""SELECT 
    '{database}' AS db,
    f.id AS familia_id,
    f.sociambiental_id,
    f.apellidos,
    s.apellidosfamilia,
    f.numeropersonas,
    s.fecha,
    s.longitud,
    s.latitud,
    s.hacinamiento,
    s.aguaservicio,
    s.diposicionexcretas,
    s.basura,
    o.familiograma,
    f.calculoapgar,
    f.apgarFuncionalidad,
    f.zaritFuncionalidad,
    f.calculozarit,
    f.lgtbi,
    f.poblacionvulnerable,
    f.cursovidafamilia,
    o.ecomapa,
    o.resultadoecomapa,
    o.dirfamiliograma,
    o.familiograma,
    o.plancuidado,
    o.dirplancuidado,
    s.id AS sociambiental_existente,
    u.microterritorio,
    u.cod_microterritorio,
    u.comuna,
    u.territorio,
    u.zona,
    r.nombres AS responsable_nombre,
    r.numero AS responsable_numero,
    r.ebs AS responsable_ebs,
    o.resultadofamiliograma,
    re.nombres AS responsable_plancuidado,
    re.numero AS resposable_plancuidado_id,
    -- 👉 Total de personas por familia (SUMA DE CURSOS DE VIDA)
    COALESCE(j.total_juventud, 0)
    + COALESCE(i.total_infantil, 0)
    + COALESCE(p.total_primera_infancia, 0)
    + COALESCE(a.total_adolescentes, 0)
    AS total_personas_cursos_vida,

    CASE
        WHEN f.sociambiental_id IS NULL THEN 'SIN_SOCIOAMBIENTAL_ID'
        WHEN s.id IS NULL THEN 'SOCIOAMBIENTAL_INVALIDO'
        ELSE 'SOCIOAMBIENTAL_OK'
    END AS estado

FROM {database}.familias f

LEFT JOIN {database}.sociambientals s 
       ON f.sociambiental_id = s.id

LEFT JOIN {database}.ubicaciones u 
       ON s.ubicacion_id = u.id

LEFT JOIN {database}.responsables r 
       ON s.responsable_id = r.id

LEFT JOIN {database}.observacions o 
       ON o.familia_id = f.id
       
LEFT JOIN {database}.responsables re
	   ON o.responsable_id = re.id

-- 👉 Subconsulta: Juventud adultos por familia
LEFT JOIN (
    SELECT familia_id, COUNT(*) AS total_juventud
    FROM {database}.juventudadultos
    GROUP BY familia_id
) j ON j.familia_id = f.id

-- 👉 Subconsulta: Infantiles por familia
LEFT JOIN (
    SELECT familia_id, COUNT(*) AS total_infantil
    FROM {database}.infantils
    GROUP BY familia_id
) i ON i.familia_id = f.id

-- 👉 Subconsulta: Primera infancia por familia
LEFT JOIN (
    SELECT familia_id, COUNT(*) AS total_primera_infancia
    FROM {database}.primerainfancias
    GROUP BY familia_id
) p ON p.familia_id = f.id

-- 👉 Subconsulta: Adolescencias por familia
LEFT JOIN (
    SELECT familia_id, COUNT(*) AS total_adolescentes
    FROM {database}.adolescencias
    GROUP BY familia_id
) a ON a.familia_id = f.id
""", cursor)
            if not familias_query:
                print(f"No se encontraron personas para {database}. Continuando.")
                continue
            acumulado_familias.extend(familias_query)
        connection.commit()

        # assign accumulated results back to `familias_query` so later code can build the DataFrame
        familias = acumulado_familias

        FE_REPORTE = datetime.now().strftime('%Y-%m-%d')

        df_familias_consolidados = pd.DataFrame(familias)
        df_observaciones_consolidados = pd.DataFrame(familias)

        df_familias_consolidados.columns = [desc[0] for desc in cursor.description]
        df_observaciones_consolidados.columns = [desc[0] for desc in cursor.description]

        # dividir reporte Observacion duplicados y no duplicados
        df_familias_consolidados.drop_duplicates(subset=['familia_id','db'], keep='first', inplace=True)


        for col in df_personas_consolidados.select_dtypes(include=['object']).columns:
            df_personas_consolidados[col] = df_personas_consolidados[col].str.strip().str.replace(r'[^\w\s]', '', regex=True)

        print("Limpieza de datos completada.")
        df_familias_consolidados['longitud'] = df_familias_consolidados['longitud'].apply(limpiar_formato_longitud)
        df_familias_consolidados['latitud'] = df_familias_consolidados['latitud'].apply(limpiar_formato_latitud)



        for i, col in enumerate(df_familias_consolidados.columns):
            if df_familias_consolidados.dtypes.iloc[i] == object and col not in ('longitud', 'latitud'):
                df_familias_consolidados.iloc[:, i] = df_familias_consolidados.iloc[:, i].astype(
                    str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

        for i, col in enumerate(df_observaciones_consolidados.columns):
            if df_observaciones_consolidados.dtypes.iloc[i] == object and col not in ('longitud', 'latitud'):
                df_observaciones_consolidados.iloc[:, i] = df_observaciones_consolidados.iloc[:, i].astype(
                    str).str.strip().str.replace(r'[^\w\s]', '', regex=True)

        df_familias_consolidados['fecha'] = pd.to_datetime(df_familias_consolidados['fecha'],
                                                           errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
            str)

        df_familias_consolidados['responsable_numero'] = (
            df_familias_consolidados['responsable_numero']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.replace(r'\.0+$', '', regex=True)
        )

        df_familias_consolidados['resposable_plancuidado_id'] = (
            df_familias_consolidados['resposable_plancuidado_id']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.replace(r'\.0+$', '', regex=True)
        )

        df_familias_consolidados['validacion'] = ''
        mask_missing = df_familias_consolidados['longitud'].isna() | df_familias_consolidados['latitud'].isna()
        df_familias_consolidados.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (COORDENADAS INVALIDAS)'

        mask_missing = df_familias_consolidados['total_personas_cursos_vida'] == 0
        df_familias_consolidados.loc[mask_missing, 'validacion'] = 'ERROR EN CARACTERIZACION (SIN INTEGRANTES)'

        url = "https://docs.google.com/spreadsheets/d/1qChAneFYqnsUHOMPrxgPLNY36vqn-bmtMq6qnVTrYI4/export?format=csv&gid=0"

        df_distribucion_redes = pd.read_csv(url)

        df_familias_consolidados['redes'] = df_familias_consolidados['territorio'].map(
            df_distribucion_redes.set_index('TERRITORIO')['RED']
        ).fillna('')

        # quiero contar cuantos registros hay por estado
        total_rows = len(df_personas_consolidados)

        acumulado_novedades = []
        cursor = connection.cursor()
        for database in DATABASE:
            novedades_query = ejecutar_consulta_mysql(f"""SELECT 
            '{database}' AS db,
            v.id AS visita_negada_id,
            u.microterritorio,
            u.cod_microterritorio,
            u.comuna,
            u.territorio,
            r.nombres AS responsable_nombre,
            r.numero AS responsable_numero,
            r.ebs AS responsable_ebs,
            v.longitud,
            v.latitud,
            v.estadocasa,
            v.fecha 
        FROM {database}.visitasnegadas v 

        LEFT JOIN {database}.ubicaciones u 
               ON v.ubicacion_id = u.id

        LEFT JOIN {database}.responsables r 
               ON v.responsable_id = r.id
        """, cursor)
            if not novedades_query:
                print(f"No se encontraron personas para {database}. Continuando.")
                continue
            acumulado_novedades.extend(novedades_query)
        connection.commit()

        df_novedades_consolidado = pd.DataFrame(acumulado_novedades)
        df_novedades_consolidado.columns = [desc[0] for desc in cursor.description]

        for i, col in enumerate(df_novedades_consolidado.columns):
            if df_novedades_consolidado.dtypes.iloc[i] == object:
                df_novedades_consolidado.iloc[:, i] = df_novedades_consolidado.iloc[:, i].astype(
                    str).str.strip().str.replace(r'[^\w\s]', '', regex=True)


        os.makedirs(F'reportes/{FE_REPORTE}/looker', exist_ok=True)
        df_personas_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_personas_{FE_REPORTE}.csv')
        df_familias_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_familias_{FE_REPORTE}.csv')
        df_novedades_consolidado.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_novedades_{FE_REPORTE}.csv')
        df_observaciones_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_observaciones_{FE_REPORTE}.csv')

        # Convertir Timestamps a string
        reescribir_hoja("1HbJo2ZINdgZshcAIj7I1u-azaXPZHd1KbNdsdTASQGI", "cosolidado_familias", df_familias_consolidados, client)


    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()