import os

import ocrmypdf

def unir_pdf_carpeta( carpeta, pdf_salida):
    """
    Une todos los archivos PDF en la carpeta especificada en un solo PDF.
    """
    print(f"Uniendo PDFs en la carpeta: {carpeta}")
    pdfs = [os.path.join(carpeta, f) for f in os.listdir(carpeta) if f.endswith('.pdf')]
    pdfs.sort()  # Ordenar alfabéticamente

    if not pdfs:
        print("No se encontraron archivos PDF en la carpeta.")
        return

    # Crear un nuevo PDF vacío
    with open(pdf_salida, 'wb') as salida:
        for pdf in pdfs:
            with open(pdf, 'rb') as entrada:
                salida.write(entrada.read())

    print(f"¡Proceso completado! PDF unido guardado en: {pdf_salida}")

def convertir_pdf_a_buscable(pdf_entrada, pdf_salida, idioma='spa'):
    """
    Toma un PDF escaneado y lo convierte en un PDF buscable y seleccionable.
    """
    print("Procesando PDF...")

    # ⚠️ Los dos primeros argumentos van DIRECTOS sin 'input_file=' o 'output_file='
    ocrmypdf.ocr(
        pdf_entrada,
        pdf_salida,
        language=idioma,

        # --- OPCIONES DE ALTA PRECISIÓN Y LIMPIEZA ---
        force_ocr=True,  # Fuerza el OCR aunque detecte algo de texto previo
        deskew=True,  # Endereza páginas torcidas/inclinadas automáticamente
        clean=False,  # Limpia sombras, manchas de escaneo y fondo rugoso
        rotate_pages=True,  # Rota páginas invertidas automáticamente

        # --- OPTIMIZACIÓN DE SALIDA ---
        optimize=1,  # Comprime y optimiza el tamaño del PDF resultante
        output_type='pdf'  # Mantiene formato PDF
    )

    print(f"¡Proceso completado! PDF buscable guardado en: {pdf_salida}")

# --- EJECUCIÓN ---
if __name__ == "__main__":
    archivo_entrada = r"C:\Users\Esteban Getial\OneDrive\escaneos\2026000385.pdf"
    archivo_salida = r"C:\Users\Esteban Getial\OneDrive\escaneos\documento_buscable.pdf"

    carpeta = r"D:\1893"

    # for archivo in os.listdir(carpeta):
    #     if archivo.endswith(".pdf"):
    #         archivo_entrada = os.path.join(carpeta, archivo)
    #         archivo_salida = os.path.join(carpeta, f"buscable_{archivo}")
    #         convertir_pdf_a_buscable(archivo_entrada, archivo_salida)


    unir_pdf_carpeta(carpeta, os.path.join(carpeta, "SER124SREC20260731NI000900091143ID2177823635D03.pdf"))


