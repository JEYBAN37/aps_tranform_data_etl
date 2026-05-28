import pandas as pd
import re

def limpiar_y_extraer(texto, posicion):
    if pd.isna(texto):
        return None

    # 1. Dividir el texto por el signo $
    partes = [p.strip() for p in str(texto).split('$')]

    # Si la posición que buscamos no existe en este registro, salimos
    if posicion >= len(partes):
        return None

    opcion = partes[posicion]

    # 2. Limpiar números, puntos y pipes (|)
    opcion_limpia = re.sub(r'[0-9|.]', '', opcion).strip()

    # 3. Si empieza por "No", "Ninguno", "SD", etc., lo ignoramos (devolvemos None/Null)
    if re.match(r'^(No|No aplica|No requiere|SD|Ninguno|\\s*$)', opcion_limpia, re.IGNORECASE):
        return None

    # 4. Agrupar y estandarizar según tu diccionario de la ruta PYMS
    if re.search(r'(Medico|medicina general|Atención en salud de promoción|recién nacido|Urgencias)', opcion_limpia,
                 re.IGNORECASE):
        return 'Medicina General / Urgencias / PYMS'
    elif re.search(r'(Odontolog|Salud oral)', opcion_limpia, re.IGNORECASE):
        return 'Odontología / Salud Oral'
    elif re.search(r'(Enfermera|Enfermeria|lactancia)', opcion_limpia, re.IGNORECASE):
        return 'Enfermería PYM / Lactancia'
    elif re.search(r'(Vacunacion)', opcion_limpia, re.IGNORECASE):
        return 'Vacunación'
    elif re.search(r'(Citologia|cancer|riesgo cardiovascular|Tamizaje|VIH|Prueba rapida)', opcion_limpia,
                   re.IGNORECASE):
        return 'Tamizajes, Citología y VIH'
    elif re.search(r'(anticoncepcion|Planificación|preservativos)', opcion_limpia, re.IGNORECASE):
        return 'Planificación Familiar y Anticoncepción'
    elif re.search(r'(Tramite de autorizacion)', opcion_limpia, re.IGNORECASE):
        return 'Trámites Administrativos'
    elif re.search(r'(Valoración Integral)', opcion_limpia, re.IGNORECASE):
        return 'Valoración Integral PYMS'
    elif re.search(r'(violencias|sospecha)', opcion_limpia, re.IGNORECASE):
        return 'Ruta Violencias de Género'

    return opcion_limpia if opcion_limpia != "" else None