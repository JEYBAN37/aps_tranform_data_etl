import os
import shutil
from pdf2image import convert_from_path
from PIL import Image

from automatizacion_looker_aps.contratacion_cruzada.convertir_contratacion import crear_pdf_maestro_agreesivo


def convertir_pdf_a_gris(ruta_archivo_pdf):
    """
    Toma un archivo PDF, lo convierte a escala de grises y lo sobrescribe
    en su misma ubicación de destino.
    """
    # Usamos la ruta real del archivo que entra por parámetro
    directorio, nombre_archivo = os.path.split(ruta_archivo_pdf)

    print(f"🔄 Ajustando a escala de grises: {nombre_archivo}...")

    try:
        # 1. Convertir las páginas del PDF en imágenes usando tu ruta de Poppler
        paginas = convert_from_path(
            ruta_archivo_pdf,
            dpi=150,
            poppler_path=r"E:\Release-26.02.0-0\poppler-26.02.0\Library\bin"
        )

        paginas_grises = []

        # 2. Procesar cada página para pasarla a Blanco y Negro (Escala de grises)
        for pagina in paginas:
            # Modo 'L' = Escala de grises de 8 bits
            pagina_gris = pagina.convert("L")
            paginas_grises.append(pagina_gris)

        # 3. Sobrescribir el archivo original en la carpeta unificada con la versión en gris
        if paginas_grises:
            paginas_grises[0].save(
                ruta_archivo_pdf,  # Al pasarle la misma ruta, reemplaza el archivo a color por el gris
                save_all=True,
                append_images=paginas_grises[1:],
                resolution=100.0,
                quality=80
            )
            print(f"✅ ¡Éxito! Archivo optimizado en gris: {nombre_archivo}")
        else:
            print(f"⚠️ El PDF {nombre_archivo} no contenía páginas válidas.")

    except Exception as e:
        print(f"❌ Error al procesar el filtro gris para {nombre_archivo}: {e}")

CARPETAS = [
        "H:/1778/actas y certificados/1778",
        "H:/1778/actas y certificados/Esteban/1778",
        "H:/1778/actas y certificados/contratacion_cruzada/1778",
        "H:/1778/actas y certificados/res/1778",
    ]

DESTINO_ACTAS = "H:/1778/actas y certificados/actas_unificadas"
DESTINO_SUPERVISION = "H:/1778/actas y certificados/supervision_unificada"
DESTINO_CONTRATOS = "H:/1778/actas y certificados/contratos_unificados"


def unificar_archivos():
    print("Iniciando proceso de unificación de archivos APS...")

    # 1. Crear las carpetas de destino si aún no existen
    os.makedirs(DESTINO_ACTAS, exist_ok=True)
    os.makedirs(DESTINO_SUPERVISION, exist_ok=True)

    # Contadores
    contador_contratos_con_actas = 0
    contador_contratos_con_supervision = 0

    for carpeta in CARPETAS:
        if not os.path.exists(carpeta):
            print(f"⚠️ La ruta no existe, saltando: {carpeta}")
            continue

        print(f"\n📁 Procesando carpeta origen: {carpeta}")

        for carpeta_contrato in os.listdir(carpeta):
            ruta_contrato = os.path.join(carpeta, carpeta_contrato)

            if os.path.isdir(ruta_contrato):

                for archivo in os.listdir(ruta_contrato):
                    ruta_archivo_origen = os.path.join(ruta_contrato, archivo)

                    # 📄 Procesar ACTAS
                    if archivo.startswith("ACTA") and archivo.lower().endswith(".pdf"):
                        ruta_archivo_destino = os.path.join(DESTINO_ACTAS, archivo)

                        # Copia el archivo original a la carpeta agrupada
                        shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)
                        contador_contratos_con_actas += 1
                        print(f"  » 📑 ACTA unificada: {archivo}")

                        # 🚀 EJECUTAR FILTRO GRIS: Pasa la ruta del archivo copiado para que lo convierta ahí mismo
                        convertir_pdf_a_gris(ruta_archivo_destino)

                    # 📎 Procesar SUPERVISIONES
                    if archivo.startswith("SUPERVISION") and archivo.lower().endswith(".pdf"):
                        ruta_archivo_destino = os.path.join(DESTINO_SUPERVISION, archivo)

                        # Copia el archivo original a la carpeta agrupada
                        shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)
                        contador_contratos_con_supervision += 1
                        print(f"  » 📎 SUPERVISIÓN unificada: {archivo}")

                        # 🚀 EJECUTAR FILTRO GRIS (Opcional): Si también quieres las supervisiones en gris, descomenta la línea de abajo
                        convertir_pdf_a_gris(ruta_archivo_destino)

                    if archivo.startswith("CTO") and archivo.lower().endswith(".pdf"):
                        ruta_archivo_destino = os.path.join(DESTINO_CONTRATOS, archivo)

                        # Copia el archivo original a la carpeta agrupada
                        shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)
                        contador_contratos_con_supervision += 1
                        print(f"  »  CONTRATOS unificada: {archivo}")

                        # 🚀 EJECUTAR FILTRO GRIS (Opcional): Si también quieres las supervisiones en gris, descomenta la línea de abajo
                        convertir_pdf_a_gris(ruta_archivo_destino)

    print("\n" + "=" * 40)
    print("¡Proceso de unificación y conversión completado!")
    print(f"Total Actas unificadas y en gris: {contador_contratos_con_actas}")
    print(f"Total Supervisiones unificadas y en gris: {contador_contratos_con_supervision}")
    print("=" * 40)


if __name__ == "__main__":
    # Ahora que el flujo está conectado de forma dinámica, puedes ejecutar la función principal directamente
    # unificar_archivos()
    crear_pdf_maestro_agreesivo(carpeta_base='E:/ilovepdf_extracted-pages',
                                nombre_maestro="SER124SREC202605311NI000900091143ID2177823635D08.pdf",
                                nombre_a_unificar="index", contratos_opcion=False)
