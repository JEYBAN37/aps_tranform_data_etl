import os
import time

import mysql.connector
import pandas as pd
from google.oauth2.service_account import Credentials
from pandas.io.gbq import to_gbq
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, DATABASE_APS2024, MYSQL_REPLICA_PASSWORD, DATABASE
from export_aps_124 import limpiar_formato_latitud, limpiar_formato_longitud
from mysql_conector import ejecutar_consulta_mysql
from datetime import datetime
import gspread
import firebase_admin
from firebase_admin import credentials, firestore


def cargar_csv_a_bigquery(df, table_id, project_id, columnas_fecha=None):
    df = df.copy()

    # Normalizar nombres de columnas
    df.columns = df.columns.astype(str)

    # Quitar columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    # Resetear índice (CRÍTICO)
    df.reset_index(drop=True, inplace=True)

    # Convertir columnas de fecha explícitamente
    if columnas_fecha:
        for col in columnas_fecha:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convertir SOLO objetos que no sean fecha
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="replace"
    )

def reescribir_hoja(sheet_id, sheet_name, df, client):
    df = df.copy()

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    sheet.clear()

    for col in df.columns:
        # Caso 1: columna datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            df[col] = df[col].where(df[col].notna(), "")

        # Caso 2: columna no datetime
        else:
            df[col] = df[col].map(
                lambda x:
                    "" if x is None or (isinstance(x, float) and pd.isna(x))
                    else x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (pd.Timestamp, datetime))
                    else str(x)
            )

    data = [df.columns.tolist()] + df.values.tolist()
    sheet.update(range_name="A1", values=data)

def actualizar_cedulas_firebase(df_personas, cred):
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    collection = db.collection("personas")

    BATCH_SIZE = 200
    SLEEP = 0.7

    batch = db.batch()
    count = 0

    df = pd.DataFrame([{
        'cedula': row['doc_id'],
        'familia': row['familia_id'],
        'fecha': row['fecha'],
        'edad': row['edad'],
        'nombre': row['primerapellido'] + ' ' + row['segundoapellido'] + ' ' + row['primernombre'] + ' ' + row[
            'segundonombre'],
        'telefono': row['celular'],
        'vivienda': row['sociambiental_id'],
    } for _, row in df_personas.iterrows()
    ])

    df = df[df["cedula"].notna()]  # elimina NaN
    df["cedula"] = df["cedula"].astype(str).str.strip()
    df["cedula"] = df["cedula"].astype(str).str.strip().apply(lambda s: s[1:] if s.startswith("0") else s)
    df = df[df["cedula"] != ""]  # elimina vacíos
    df = df.drop_duplicates(subset=["cedula"])
    df = df.set_index("cedula")
    data = df.to_dict(orient="index")

    total = len(df)
    print(f"🚀 Iniciando carga de {total} personas")

    for i, row in df.iterrows():
        cedula = str(row["cedula"]).strip()

        if not cedula or cedula.lower() == "nan":
            continue

        doc_ref = collection.document(cedula)

        data = row.dropna().to_dict()

        batch.set(doc_ref, data, merge=True)

        count += 1

        if count % BATCH_SIZE == 0:
            try:
                batch.commit()
                print(f"✅ Insertados {count}/{total}")
                time.sleep(SLEEP)
            except Exception as e:
                print(f"⚠️ Error en batch {count}: {e}")
                time.sleep(2)
            batch = db.batch()

    # último batch
    if count % BATCH_SIZE != 0:
        batch.commit()
        print(f"✅ Insertados {count}/{total}")

    print("🎉 Carga finalizada sin errores")

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
                t.* ,
                s.id as sociambiental_id,
                s.fecha,
                s.barriovereda,
                s.direccion,
                f.celular,
                s.vivienda,
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
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        valoracionmedica AS valoracion,
        saludoral AS higiene_oral,
        aseguradora,
        regimen,
        metodosanticonceptivos,
        infeccionestransmisionsexual,
        controlprenatal AS controlP,
        consumospa,
        tomacitologia,
        mamografia,
        discapacidad,
        fechanac,
        sexo,
        'NO APLICA' AS desnutricion,
        iniciovidasexual,
        riesgoembarazo,
        canalizacionuno,
        sopechamaltrato,
        'NO APLICA' AS desarrolloinfantil,
        'Adulto' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.juventudadultos
    LEFT JOIN {database}.canalizaciones c ON c.id = juventudadultos.canalizacion_id

    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        'NO APLICA' AS gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        crecimientoydesarrollo AS valoracion,
        higieneoral AS higiene_oral,
        aseguradora,
        regimen,
        'NO APLICA' AS metodosanticonceptivos,
        'NO APLICA' AS infeccionestransmisionsexual,
        'NO APLICA' AS controlP,
        'NO APLICA' AS consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        desnutricion,
        'NO APLICA' AS iniciovidasexual,
        'NO APLICA' AS riesgoembarazo,
        canalizacionuno,
        'NO APLICA' AS sopechamaltrato,
        desarrolloinfantil,
        'Infante' AS cursodevida,
         c.nombre,
         estadocanalizacion
    FROM {database}.infantils
    LEFT JOIN {database}.canalizaciones c ON c.id = infantils.canalizacion_id
    
    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        'NO APLICA' AS gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        crecimientoydesarrollo AS valoracion,
        higieneoral AS higiene_oral,
        aseguradora,
        regimen,
        'NO APLICA' AS metodosanticonceptivos,
        'NO APLICA' AS infeccionestransmisionsexual,
        'NO APLICA' AS controlP,
        'NO APLICA' AS consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        desnutricion,
        'NO APLICA' AS iniciovidasexual,
        'NO APLICA' AS riesgoembarazo,
        canalizacionuno,
        'NO APILCA' AS sopechamaltrato,
        desarrolloinfantil,
        'PrimeraInfancia' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.primerainfancias
    LEFT JOIN {database}.canalizaciones c ON c.id = primerainfancias.canalizacion_id

    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        valoracionmedica AS valoracion,
        saludoral AS higiene_oral,
        aseguradora,
        regimen,
        metodosanticonceptivos,
        infeccionestransmisionsexual,
        controlprenatal AS controlP,
        consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        'NO APLICA' AS desnutricion,
        iniciovidasexual,
        riesgoembarazo,
        canalizacionuno,
        sopechamaltrato,
        'NO APLICA' AS desarrolloinfantil,
        'Adolescencia' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.adolescencias
    LEFT JOIN {database}.canalizaciones c ON c.id = adolescencias.canalizacion_id
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
    s.base_anterior AS db,
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
    s.vivienda,
    s.direccion,
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
    o.plancuidado,
    o.dirplancuidado,
    o.date,
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
    END AS estado,
    f.numerodocumento AS representante_doc_id,
    f.rol,
    f.celular,
    s.estrato,
    s.numerohabitantes,
    f.tipofamilia,
    f.cursovidafamilia,
    s.riesgoexterno,
    s.vacunamascotas,
    s.vector,
    s.riesgo,
    f.antecedenteenfermedad,
    f.riesgopsicosocial,
    f.estilodevidapredominante,
    f.cepilladodientes,
    f.higiene

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

        df_personas_consolidados['familia_id'] = (
            df_personas_consolidados['familia_id']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.replace(r'\.0+$', '', regex=True)
        )

        df_personas_consolidados['sociambiental_id'] = (
            df_personas_consolidados['sociambiental_id']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.replace(r'\.0+$', '', regex=True)
        )

        df_personas_consolidados['numero'] = (
            df_personas_consolidados['numero']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.replace(r'\.0+$', '', regex=True)
        )

        for i, col in enumerate(df_personas_consolidados.columns):
            if df_personas_consolidados.dtypes.iloc[i] == object and col not in ('canalizacionuno'):
                df_personas_consolidados[col] = df_personas_consolidados[col].fillna('').astype(
                    str).str.strip().str.replace(r'[^\w\s]', '', regex=True)


        df_personas_consolidados['canalizacionuno'] = (
            df_personas_consolidados['canalizacionuno']
            .str.replace(',', '$', regex=True)
        )

        df_personas_consolidados['fecha'] = pd.to_datetime(df_personas_consolidados['fecha'],
                                                           errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
            str)

        df_personas_consolidados['fechanac'] = pd.to_datetime(df_personas_consolidados['fechanac'], errors='coerce')
        today = pd.to_datetime('today').normalize()

        def _calc_age(birth):
            if pd.isna(birth):
                return ''
            return str(today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day)))

        df_personas_consolidados['edad'] = df_personas_consolidados['fechanac'].apply(_calc_age)
        df_personas_consolidados['fechanac'] = df_personas_consolidados['fechanac'].dt.strftime('%Y-%m-%d').fillna(
            '').astype(str)

        df_personas_consolidados['doc_id'] = df_personas_consolidados['doc_id'].astype(str).str.strip().str.replace(
            r'\D+', '', regex=True)

        print("Limpieza de datos completada.")
        df_familias_consolidados['longitud'] = df_familias_consolidados['longitud'].apply(limpiar_formato_longitud)
        df_familias_consolidados['latitud'] = df_familias_consolidados['latitud'].apply(limpiar_formato_latitud)



        for i, col in enumerate(df_familias_consolidados.columns):
            if df_familias_consolidados.dtypes.iloc[i] == object and col not in ('longitud', 'latitud','familiograma','plancuidado'):
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

        df_personas_consolidados['redes'] = df_personas_consolidados['territorio'].map(
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

        LEFT JOIN agsolutic_aps2024.ubicaciones u 
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

        df_novedades_consolidado['fecha'] = pd.to_datetime(df_novedades_consolidado['fecha'],
                                                           errors='coerce').dt.strftime('%Y-%m-%d').fillna('').astype(
            str)

        df_novedades_consolidado['redes'] = df_novedades_consolidado['territorio'].map(
            df_distribucion_redes.set_index('TERRITORIO')['RED']
        ).fillna('')


        os.makedirs(F'reportes/{FE_REPORTE}/looker', exist_ok=True)
        df_personas_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_personas_{FE_REPORTE}.csv')

        df_personas_consolidados.to_csv(F'crucez/base_actualizada/cosolidado_personas_{FE_REPORTE}.csv', index=False)

        df_familias_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_familias_{FE_REPORTE}.csv')
        df_novedades_consolidado.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_novedades_{FE_REPORTE}.csv')
        df_observaciones_consolidados.to_csv(
            F'reportes/{FE_REPORTE}/looker/cosolidado_observaciones_{FE_REPORTE}.csv')




        # Convertir Timestamps a string
        df_familias_consolidados['reporte_fecha'] = FE_REPORTE
        df_personas_consolidados['reporte_fecha'] = FE_REPORTE
        df_novedades_consolidado['reporte_fecha'] = FE_REPORTE

        print(df_familias_consolidados.head())
        print(df_familias_consolidados.shape)

        cargar_csv_a_bigquery(df_personas_consolidados, table_id="datos_aps.personas",project_id="aps-project-478903",columnas_fecha=['fechanac','fecha','reporte_fecha'])
        cargar_csv_a_bigquery(df_familias_consolidados, table_id="datos_aps.familias", project_id="aps-project-478903",columnas_fecha=['date','fecha','reporte_fecha'])
        cargar_csv_a_bigquery(df_novedades_consolidado, table_id="datos_aps.novedades", project_id="aps-project-478903",columnas_fecha=['fecha','reporte_fecha'])

        reescribir_hoja("1HbJo2ZINdgZshcAIj7I1u-azaXPZHd1KbNdsdTASQGI", "cosolidado_familias", df_familias_consolidados, client)
        reescribir_hoja("1g6865j3cOGhkj6VAkfIqcJqScB4eWTXUqrZx16Czhuo", "cosolidado_personas", df_personas_consolidados, client)
        reescribir_hoja("1Yr9gvmWQ7i6ANgI-9LAW8Yfwi6HScJfF7ll3nm0StTE", "cosolidado_novedades", df_novedades_consolidado, client)

        #actualizar_cedulas_firebase(df_personas_consolidados, credentials.Certificate("aps-run-id-firebase-adminsdk-fbsvc-9f8e9a6e72.json"))

    except Exception as e:

        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()