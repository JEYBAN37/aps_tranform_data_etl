import numpy as np
import pandas as pd
from datetime import datetime

def sobrescribir_hoja(sheet_id, sheet_name, df, client):
    df = df.copy()

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    sheet.clear()

    # Helper to convert any cell to a JSON/Sheets-safe string
    def _cell_to_string(x):
        # NA or None
        if x is None:
            return ""
        # Pandas NA / numpy NaT
        try:
            if pd.isna(x):
                return ""
        except Exception:
            pass

        # Datetime-like objects
        if isinstance(x, (pd.Timestamp, datetime, np.datetime64)):
            try:
                # pd.Timestamp and datetime support strftime
                return pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return str(x)

        # Otherwise convert to str
        return str(x)

    # Apply conversion row-wise (safe for mixed dtypes including object columns containing timestamps)
    # Build list of lists where first row is headers
    headers = [str(c) for c in df.columns.tolist()]
    rows = []
    for _, row in df.iterrows():
        converted = [_cell_to_string(v) for v in row.tolist()]
        rows.append(converted)

    data = [headers] + rows

    # Use named arguments to follow new worksheet.update signature
    sheet.update(values=data, range_name="A1")