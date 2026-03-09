import pandas as pd
import json

def convertir_to_json():
    sheet_id = "1dqkisXc5OQNKTWd4YgMKnW5vu382FSR6"
    sheet_name = "CRONOGRAMA_SEMANA_6"  # El nombre de la pestaña

    # Formateamos la URL para descargar como CSV
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    df = pd.read_csv(url)
    df['id'] = df.index + 1

    # Convert common date columns to a JS-friendly array: [year, monthIndex, day, hour, minute]
    date_columns = ['fechaInicio', 'fechaFin', 'fecha_inicio', 'fecha_fin', 'start', 'end']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            def to_js_array(dt):
                if pd.isna(dt):
                    return None
                return [int(dt.year), int(dt.month), int(dt.day), int(dt.hour), int(dt.minute)]
            df[col] = df[col].apply(to_js_array)

    df['territorio'] = df['territorio'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['celular'] = df['celular'].apply(
        lambda x: None if pd.isna(x) else (lambda s: s[:-2] if s.endswith(",00") else s)(str(x).replace(" ", "")))
    df['celular'] = df['celular'].apply(lambda x: None if pd.isna(x) else str(x).replace(".", ""))
    df['celular'] = df['celular'].apply(lambda x: None if pd.isna(x) else str(x).replace(",", ""))
    df['db'] = df['db'].apply(lambda x: None if pd.isna(x) else str(x).replace(" ", ""))
    df['descripcion'] = df['descripcion'].apply(lambda x: "" if pd.isna(x) else str(x).replace(" ", ""))

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
    #main()
    #cargar_indicadores()
    #unir_csv_falla_coordenadas()
    #unir_csv_falla_familias()
    convertir_to_json()