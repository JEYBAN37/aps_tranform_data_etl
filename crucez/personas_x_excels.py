import pandas as pd
from networkx import dfs_edges


def cruzar_personas_con_excels():

    df_personas = pd.read_csv('base_actualizada/cosolidado_personas_2026-01-21.csv', dtype=str)

    df_personas.loc['doc_id'] = df_personas['doc_id'].str.strip()


    df_comparativo = pd.read_excel('excel/BD_SEGUIMIENTOsub_NO_sisben_ aps.xlsx', dtype=str)

    df_comparativo['ND'] = df_comparativo['ND'].str.strip()

    df_consolodidado = df_personas[df_personas['doc_id'].isin(df_comparativo['ND'])]

    df_entregable = pd.merge(
        df_comparativo,  # Columnas del A
        df_personas,  # Columnas del B
        left_on='ND',
        right_on='doc_id',
        how='inner'  # Solo los que coinciden en ambos
    ).drop(columns=['doc_id'])  # Borramos la columna repetida

    print(df_consolodidado.head())

    df_entregable.to_excel('personas_x_excels_cruzado_2026-01-21.xlsx', index=False)

if __name__ == "__main__":
    cruzar_personas_con_excels()