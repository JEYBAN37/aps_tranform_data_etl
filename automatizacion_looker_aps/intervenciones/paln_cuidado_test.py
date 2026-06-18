import pandas as pd

from automatizacion_looker_aps.intervenciones.filtrado_actividades import extrer_variable_familia, \
    verificar_actualizacion_ficha

if __name__ == "__main__":

    responseTimes = [100, 200, 150, 300]

    longitud_array = len(responseTimes)


    acumulador = responseTimes[0]
    contador = 0

    for i, day in enumerate(responseTimes[1:], start=1):
        average = acumulador / i
        if day > average:
            contador += 1
        acumulador += day
        print(acumulador)




    prueba = pd.DataFrame({
        'familia_id': [79769, 79769, 79761, 79769],
        'conteo_plan_cuidado': ['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'],
        'id_familia_plan': [79769, 79769, 79761, 79769],
        'fecha': ['2026-04-16', '2026-04-16', '2026-03-28', '2026-03-28'],
        'db': ['db1', 'db1', 'db2', 'db1']
    })

    df_familia = pd.DataFrame({
        'familia_id': [79769, 79769, 79761, 79769],
        'conteo_plan_cuidado': ['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'],
        'id_familia_plan': [79769, 79769, 79761, 79769],
        'fecha': ['2026-04-16', '2026-04-16', '2026-03-28', '2026-03-28'],
        'db': ['db1', 'db1', 'db2', 'db1']
    })

    df_actividades_consolidados = pd.DataFrame({
        'familia_id': [79769, 79769, 79761, 79769],
        'conteo_plan_cuidado': ['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'],
        'id_familia_plan': [79769, 79769, 79761, 79769],
        'fecha': ['2026-04-16', '2026-04-16', '2026-03-28', '2026-03-28']
    })

    df_actividades_consolidados['db_anterior'] = df_actividades_consolidados.apply(
        lambda row: extrer_variable_familia(row, df_familia, variable='db'), axis=1
    )



    # Error: float() argument must be a string or a real number, not 'NoneType'
    verificar_actualizacion_ficha()

    # = filtrar_planes_cuidado(prueba, filtrar_por_tipo='PLAN DE CUIDADO FIRMADO')
    #df_planes_firmados = filtrar_planes_cuidado(prueba, filtrar_por_tipo='PLAN DE CUIDADO CREADO')

    #df_final = pd.concat([df_palnes_creados, df_planes_firmados], ignore_index=True)

    print(df_actividades_consolidados)


