import pandas as pd


def procesar_base():

    url ="bases_downloads/Informe de Produccion Servicio Plan Unidad (13).xlsx"
    produccion = pd.read_excel(url, skiprows=2)
    # Luego eliminamos las primeras 2 columnas si no las necesitas
    df_produccion = produccion.iloc[:, 2:]
    print(df_produccion.head())



if __name__ == "__main__":
    procesar_base()