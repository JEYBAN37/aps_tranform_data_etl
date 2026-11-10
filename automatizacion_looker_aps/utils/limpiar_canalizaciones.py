
import pandas as pd

# ── REGLAS DE DATOS (el orden importa: gana la primera que coincida) ──
CATEGORIAS = [
    (r"medico|medicina general|atencion en salud de promocion|recien nacido|urgencias",
        "Medicina General / Urgencias / PYMS"),
    (r"odontolog|salud oral",
        "Odontología / Salud Oral"),
    (r"enfermera|enfermeria|lactancia",
        "Enfermería PYM / Lactancia"),
    (r"vacunacion",
        "Vacunación"),
    (r"citologia|cancer|riesgo cardiovascular|tamizaje|vih|prueba rapida",
        "Tamizajes, Citología y VIH"),
    (r"anticoncepcion|planificacion|preservativos",
        "Planificación Familiar y Anticoncepción"),
    (r"tramite de autorizacion",
        "Trámites Administrativos"),
    (r"valoracion integral",
        "Valoración Integral PYMS"),
    (r"violencias|sospecha",
        "Ruta Violencias de Género"),
]

# valores que significan "sin canalización" (con límite de palabra)
PATRON_IGNORAR = r"^(?:no aplica|no requiere|no|sd|ninguno)\b"


def _clasificar(serie: pd.Series) -> pd.Series:
    """Recibe una Serie de textos y devuelve la categoría estandarizada."""
    limpio = (serie.str.strip()
                   .str.replace(r"[0-9|.]", "", regex=True)
                   .str.strip())

    # versión para comparar: sin tildes y en minúsculas
    norm = (limpio.str.normalize("NFKD")
                  .str.encode("ascii", "ignore").str.decode("ascii")
                  .str.lower())

    # por defecto devuelve el texto limpio (o nulo si quedó vacío)
    resultado = limpio.where(limpio.notna() & (limpio != ""))

    # aplicar reglas en reversa para que la primera regla de la lista gane
    for patron, categoria in reversed(CATEGORIAS):
        resultado = resultado.mask(norm.str.contains(patron, regex=True, na=False), categoria)

    # los valores "ignorar" ganan sobre todo
    return resultado.mask(norm.str.match(PATRON_IGNORAR, na=False))


def separar_canalizaciones(df: pd.DataFrame, col="canalizacionuno", n=3) -> pd.DataFrame:
    df = df.copy()
    df[col] = df[col].str.replace(",", "$", regex=False)

    # un solo split para las 3 columnas (antes se hacía 3 veces por fila)
    partes = df[col].str.split("$", expand=True, regex=False).reindex(columns=range(n))

    for i in range(n):
        df[f"canalizacion_{i+1}"] = _clasificar(partes[i])
    return df