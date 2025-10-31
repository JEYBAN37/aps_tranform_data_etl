from export_aps_124 import limpiar_formato_latitud


def main():
    numero = limpiar_formato_longitud('-77.2278862')
    numero_lat = limpiar_formato_latitud('1.222630')

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