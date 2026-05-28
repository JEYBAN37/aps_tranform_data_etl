import pandas as pd

from automatizacion_looker_aps.intervenciones.filtrado_actividades import filtrar_planes_cuidado

if __name__ == "__main__":

    prueba = pd.DataFrame({
        'conteo_plan_cuidado': ['PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO FIRMADO', 'PLAN DE CUIDADO CREADO'],
        'id_familia_plan': [79769, 79769, 79761, 79769],
        'fecha': ['2026-04-16', '2026-04-16', '2026-03-28', '2026-03-28']
    })


    df_palnes_creados = filtrar_planes_cuidado(prueba, filtrar_por_tipo='PLAN DE CUIDADO FIRMADO')
    df_planes_firmados = filtrar_planes_cuidado(prueba, filtrar_por_tipo='PLAN DE CUIDADO CREADO')

    df_final = pd.concat([df_palnes_creados, df_planes_firmados], ignore_index=True)

    print(df_palnes_creados)
