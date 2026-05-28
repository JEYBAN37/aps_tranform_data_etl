import pandas as pd


def verificar_indicadores(df_intervenciones):
    # Verificar que no haya valores nulos en columnas críticas

    # decartar nan
    df_validas = df_intervenciones.dropna(subset=['actividaddesarrollar'])

    df_interveciones_validas =pd.DataFrame()

    for row in df_validas.itertuples():
        if row.actividaddesarrollar and row.actividaddesarrollar.strip().lower() != 'nan':
            df_interveciones_validas = df_interveciones_validas.append(row._asdict(), ignore_index=True)

    print("Verificando valores nulos en columnas críticas...")


