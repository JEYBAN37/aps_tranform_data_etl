
import pytesseract
import fitz          # PyMuPDF
from PIL import Image
import io


def extraer_texto_pdf_con_indice(ruta_pdf: str, num_pages=False) -> list[tuple[int, str]]:
    """
    Retorna lista de (num_pagina, texto) para cada página con contenido.
    Así sabemos exactamente en qué página se encontró la keyword.
    """
    doc    = fitz.open(ruta_pdf)
    paginas = []

    pages_to_check = range(len(doc)) if num_pages else [0]

    for idx in pages_to_check:
        pagina       = doc[idx]
        texto_pagina = pagina.get_text().strip()

        if texto_pagina:
            paginas.append((idx, texto_pagina))
            continue

        # Página sin texto → OCR
        matriz    = fitz.Matrix(2, 2)
        pixmap    = pagina.get_pixmap(matrix=matriz)
        imagen    = Image.open(io.BytesIO(pixmap.tobytes("png")))
        texto_ocr = pytesseract.image_to_string(imagen, lang="spa+eng").strip()

        if texto_ocr:
            paginas.append((idx, texto_ocr))

    doc.close()
    return paginas  # [(0, "texto pag 1"), (1, "texto pag 2"), ...]

def limpiar_numero_contrato(numero):
    # Eliminar espacios y caracteres no deseados
    numero_limpio = str(numero).strip()

    # Reemplazar los espacion en blanco
    numero_limpio = numero_limpio.replace(" ", "")

    # Aquí puedes agregar más reglas de limpieza si es necesario
    antes_Del_guion = numero_limpio.split("-")[0]  # Tomar solo la parte antes del guion
    if numero_limpio[0] == "0" and len(antes_Del_guion) > 3:  # Si el número comienza con "0" y tiene más de un dígito
        numero_limpio = numero_limpio[1:]
        return numero_limpio

    return numero_limpio