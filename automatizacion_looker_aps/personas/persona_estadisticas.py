import numpy as np
import pandas as pd

from automatizacion_looker_aps.personas.personas import cargar_personas
from automatizacion_looker_aps.responsables.responsable import cargar_responsables
from credenciales import MYSQL_APS, MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DATABASE, \
    DATABASE_APS2025
import mysql.connector

from limpieza_datos import extraer_distribucion_redes

import pandas as pd
import numpy as np

if __name__ == "__main__":
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
        responsables_ebs = cargar_responsables(cursor, DATABASE)
        connection.commit()

        cursor = connection.cursor()

        df_distribucuon = extraer_distribucion_redes()

        # Cargar los datos
        personas_url = r"F:\APS AUTOMATIZACIONES\aps\automatizacion_looker_aps\produccion_ese\bases_downloads\produccion_filtrada_2026_EBS_microterritorios_para_eps.csv"
        personas = pd.read_csv(personas_url)

        # ==========================================
        # 0. FILTRO LIBRE DE TERRITORIOS (Ajusta aquí)
        # ==========================================
        # Si quieres filtrar territorios específicos, ponlos en la lista.
        # Si dejas la lista vacía [], procesará todos los territorios.
        territorios_seleccionados = ['T70','T64','T61','T59','T52','T50','T13','T11','T69','T57','T55','T53','T51','T41','T63','T63','T60','T54','T24'] # Ej: ['PASTO', 'TUMACO'] o [] para todos

        if territorios_seleccionados and 'territorio' in personas.columns:
            personas = personas[personas['territorio'].isin(territorios_seleccionados)].copy()

        # 1. Asegurar formato de fecha y convertir edad a numérica
        personas['fecha_dt'] = pd.to_datetime(personas['Fecha_Servicio'], format='%d/%m/%Y', errors='coerce')
        personas['edad_num'] = pd.to_numeric(personas['Edad'], errors='coerce')

        # 2. Filtrar únicamente los registros de AGOSTO y SEPTIEMBRE de 2025 (Meses 8 y 9)
        df_filtrado = personas

        # 3. Calcular el Curso de Vida aplicando la lógica de rangos de edad
        condiciones = [
            (df_filtrado['edad_num'] <= 5),
            (df_filtrado['edad_num'] > 5) & (df_filtrado['edad_num'] <= 11),
            (df_filtrado['edad_num'] > 11) & (df_filtrado['edad_num'] <= 18),
            (df_filtrado['edad_num'] > 18) & (df_filtrado['edad_num'] <= 28),
            (df_filtrado['edad_num'] > 28) & (df_filtrado['edad_num'] <= 59),
            (df_filtrado['edad_num'] > 59)
        ]

        etiquetas = [
            "Primera Infancia",
            "Infancia",
            "Adolescencia",
            "Juventud",
            "Adultéz",
            "Vejéz"
        ]

        df_filtrado['curso_de_vida'] = np.select(condiciones, etiquetas, default='7. Sin Clasificar')


        # 4. Saneamiento y estandarización de Aseguradoras
        def limpiar_aseguradora(val):
            if pd.isna(val):
                return "NO AFILIADO / SIN DATO"

            val_str = str(val).strip().upper()

            # Manejar vacíos o no afiliados
            if val_str in ['', 'NAN', 'NINGUNA', 'NO', 'NO APLICA', 'PAPA', 'PARTICULAR']:
                return "NO AFILIADO / SIN DATO"

            # Unificar variantes comunes
            if 'NUEVA' in val_str:
                return 'NUEVA EPS'
            elif 'SANITAS' in val_str:
                return 'EPS SANITAS'
            elif 'MALLAMAS' in val_str:
                return 'EPS MIALAMAS'
            elif 'ASMET' in val_str:
                return 'ASMET SALUD'
            elif 'FAMISANAR' in val_str:
                return 'FAMISANAR'
            elif 'EMSSA' in val_str:
                return 'EMSSANAR'

            return 'OTRO'


        df_filtrado['aseguradora_limpia'] = df_filtrado['Carnet'].apply(limpiar_aseguradora)


        # 5. Crear función auxiliar para identificar canalizaciones válidas
        def validar_canalizacion(val):
            if pd.isna(val):
                return 0
            val_str = str(val).strip().lower()
            if val_str == '' or val_str == 'no aplica' or val_str == 'nan':
                return 0
            return 1

        df_filtrado['canalizacion_1'] = ''


        df_filtrado['canalizacion_valida'] = df_filtrado['canalizacion_1'].apply(validar_canalizacion)

        # 6. Agrupamiento jerárquico por Territorio, Microterritorio, Aseguradora Limpia y Curso de Vida
        df_agrupado = df_filtrado.groupby(
            ['territorio', 'microterritorio', 'aseguradora_limpia', 'curso_de_vida']
        ).agg(
            total_personas=('doc_id', 'count'),
            total_canalizaciones=('canalizacion_1', lambda x: ((x.notna()) & (x.astype(str).str.strip() != '') & (
                    x.astype(str).str.strip().str.lower() != 'no aplica')).sum()),
            total_personas_canalizadas=('canalizacion_valida', 'sum')
        ).reset_index()

        # 7. Asegurar que TODAS las combinaciones posibles de cursos de vida existan (Rellenar con ceros si no hay datos)
        # Creamos una malla con todas las combinaciones únicas de territorio, microterritorio, aseguradora y cursos de vida
        territorios = df_agrupado['territorio'].unique()
        microterritorios = df_agrupado['microterritorio'].unique()
        aseguradoras = df_agrupado['aseguradora_limpia'].unique()
        cursos_vida = etiquetas + ['7. Sin Clasificar']

        # MultiIndex / Producto cartesiano para garantizar la creación de filas faltantes
        idx = pd.MultiIndex.from_product(
            [territorios, microterritorios, aseguradoras, cursos_vida],
            names=['territorio', 'microterritorio', 'aseguradora_limpia', 'curso_de_vida']
        )



        # 8. Ordenar de forma limpia y estructurada
        df_resultado = df_agrupado.sort_values(
            by=['territorio', 'microterritorio', 'aseguradora_limpia', 'curso_de_vida']
        ).reset_index(drop=True)

        # Visualizar el resultado final
        print(df_resultado.head(30))

        # Guardar en Excel
        df_resultado.to_excel(
            r"F:\APS AUTOMATIZACIONES\aps\automatizacion_looker_aps\personas\reporte_idicadores_x_eps_x_atenciones.xlsx",
            index=False)

    except Exception as e:
        connection.rollback()  # Rollback in case of error
        print(f"Error: {e}")

    finally:
        cursor.close()
        connection.close()