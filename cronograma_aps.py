import pandas as pd
import json

def to_js_array(dt):
    # 1. Manejar nulos (NaN o NaT)
    if pd.isna(dt):
        return None

    # 2. Asegurar que sea un objeto datetime
    dt = pd.to_datetime(dt)

    # 3. Extraer la hora original
    hora = int(dt.hour)

    # 4. Aplicar tu lógica: Si es medianoche (0), cambiar a 12
    if hora == 0:
        hora_ajustada = 12

        # 5. Si la hora está entre 1 y 6 (madrugada), convertir a la tarde (PM)
    # Sumamos 12: 1 -> 13, 2 -> 14, ..., 6 -> 18
    elif 1 <= hora <= 6:
        hora_ajustada = hora + 12

    else:
        hora_ajustada = hora

    return [
        int(dt.year),
        int(dt.month),
        int(dt.day),
        hora_ajustada,
        int(dt.minute)
    ]

def convertir_to_json():
    sheet_id = "197y-WIQM_zu6pJdoDNCelnQZyTr-d9TujUzX2jTsuSY"
    sheet_name = "RESPUESTAS"  # El nombre de la pestaña


    # Formateamos la URL para descargar como CSV
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    url_jefes = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet=NUMEROS_JEFES"


    df_numero_jefes = pd.read_csv(url_jefes)[['JEFE','CELULAR']].drop_duplicates()

    df = pd.read_csv(url)
    df['id'] = df.index + 1
    df = df.merge(df_numero_jefes,  left_on="Responsable de EBS", right_on="JEFE", how="left")
    df_cargar = df[df["CELULAR"].notna()]
    df_verificar_manualmente = df[df["CELULAR"].isna()]

    df = df_cargar
    # Convert common date columns to a JS-friendly array: [year, monthIndex, day, hour, minute]
    date_columns = ['Fecha de Inicio de Actividad', 'Fecha de Finalización de Actividad']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].apply(to_js_array)


    df['territorio'] = df['Territorio asignado'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['celular'] = df['CELULAR'].apply(
        lambda x: None if pd.isna(x) else (lambda s: s[:-2] if s.endswith(",00") else s)(str(x).replace(" ", "")))
    df['celular'] = df['celular'].apply(lambda x: None if pd.isna(x) else str(x).replace(".", ""))
    df['celular'] = df['celular'].apply(lambda x: None if pd.isna(x) else str(x).replace(",", ""))
    df['db'] = df['RED'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['title'] = df['Nombre de la Actividad a Realizar'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['descripcion'] = df['Si es otra Especifique la actividad'].apply(lambda x: "" if pd.isna(x) else str(x).replace(" ", ""))
    df['equipo'] = df['Territorio asignado'].apply(lambda x: "" if pd.isna(x) else str(x).replace(" ", ""))
    df['novedad'] = df['Novedad'].apply(lambda x: "" if pd.isna(x) else str(x).replace(" ", ""))
    df['responsable'] = df['Responsable de EBS'].apply(lambda x: "" if pd.isna(x) else str(x).replace(" ", ""))
    df['start'] = df['Fecha de Inicio de Actividad']
    df['end'] = df['Fecha de Finalización de Actividad']
    df['ubicacion'] = df['Ubicación Especifica, en que parte del territorio esta']


    # If `url` column exists, ensure missing values become JSON null
    if 'url' in df.columns:
        df['url'] = df['url'].apply(lambda x: None if pd.isna(x) else x)

    # Replace any remaining NaN with None so json.dump writes null
    df = df.where(pd.notnull(df), None)

    colums = ['id', 'title', 'descripcion', 'start', 'end', 'territorio', 'celular', 'url', 'ubicacion', 'responsable',
              'novedad', 'db', 'equipo']
    for c in colums:
        if c not in df.columns:
            df[c] = None
    df = df[colums]

    # Write records with native Python structures so JSON arrays/nulls are preserved

    ruta_json = "cv/Formato_Cronograma.json"
    ruta_a_cronograma = "F:/APS AUTOMATIZACIONES/calendario/v0-calendar-activity-management/public/Formato_Cronograma.json"
    records = df.to_dict(orient='records')

    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    with open(ruta_a_cronograma, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    convertir_to_json()