import pandas as pd
import numpy as np


def main():
    URL_CONTRATACION_PLANTILLA = r"C:\Users\Esteban Getial\Downloads\29092026_Dilienciada_Preliminar_Matriz _PIC 2027-1.xlsx"

    # 1. Lectura de archivos
    df_matriz_completar = pd.read_csv(
        r"C:\Users\Esteban Getial\OneDrive\Documentos\extraccion.csv",
        sep='|'
    )
    df_extracion = pd.read_excel(
        URL_CONTRATACION_PLANTILLA,
        header=6
    )

    col_micro = 'Indicar el nombre o código único del microterritorio. Debe permitir identificarlo sin ambigüedad y evitar duplicidades.'

    # 2. Asignación del microterritorio por bloques de 6 filas (1 fila de origen + 5 vacías)
    df_extracion['microterritorio'] = np.nan
    i = 0
    total_filas = len(df_extracion)

    while i < total_filas:
        valor_actual = df_extracion.at[i, col_micro]

        if pd.notna(valor_actual) and str(valor_actual).strip() != '':
            nombre_micro = str(valor_actual).strip()
            # Asigna este microterritorio a la fila actual y a las siguientes 5 filas
            df_extracion.loc[i: i + 5, 'microterritorio'] = nombre_micro
            i += 6
        else:
            i += 1

    # 3. Limpieza de df_matriz_completar para evitar duplicaciones en el merge
    df_matriz_limpio = df_matriz_completar[['microterritorio', 'analisis_ingresos_estrato']].copy()

    # Asegurar que el campo microterritorio esté limpio en ambas tablas
    df_extracion['microterritorio'] = df_extracion['microterritorio'].astype(str).str.strip()
    df_matriz_limpio['microterritorio'] = df_matriz_limpio['microterritorio'].astype(str).str.strip()

    # Eliminar duplicados en el CSV para que la relación en el merge sea 1 a 1 o N a 1
    df_matriz_limpio = df_matriz_limpio.drop_duplicates(subset=['microterritorio'])

    # 4. Combinar la información (Merge) sin duplicar filas de df_extracion
    df_resultado = df_extracion.merge(
        df_matriz_limpio,
        on='microterritorio',
        how='left'
    )

    print(df_resultado[['microterritorio', 'analisis_ingresos_estrato']].head(18))

    # 5. Exportar el archivo final
    df_resultado.to_excel(
        r"C:\Users\Esteban Getial\OneDrive\Documentos\extraccion_actualizada.xlsx",
        index=False
    )


if __name__ == "__main__":
    main()