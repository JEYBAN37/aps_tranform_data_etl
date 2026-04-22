import random
import time
from typing import Tuple, Any

import pandas as pd
import pytesseract
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
import os
import re
import fitz  # PyMuPDF
from PIL import Image
import io

from automatizacion_looker_aps.contratacion_cruzada.extractores_texto import extraer_texto_pdf_con_indice, \
    limpiar_numero_contrato
from automatizacion_looker_aps.contratacion_cruzada.identificadores_coincidencias import contiene_keyword_indice

KEYWORD = [
    ("CONTRATO DE COMPRAVENTA", "CONTRATO_TIPO_1_"),
    ("GJ 062", "CONTRATO_TIPO_2_"),
    ("ADICION Y PRÓRROGA", "CONTRATO_TIPO_3_"),
    ("ACTA DE ADICIÓN Y PRORROGA", "CONTRATO_TIPO_4_"),
    ("CONTRATO DE PRESTACIÓN DE SERVICIOS No.", "CONTRATO_TIPO_5_"),
    ("CONTRATO DE PRESTACIÓN DE SERVICIOS No_.", "CONTRATO_TIPO_8_"),
    ("CONTRATO DE PRESTACIÓN DE SERVICIOS", "CONTRATO_TIPO_9_"),
    ("ACTA DE PRÓRROGA", "CONTRATO_TIPO_7_"),
]  # Cambia esto por la palabra clave que quieras buscar en los PDFs
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

BASE_SECOP = "bases/SECOP_II_-_Procesos_de_Contratación_20260327.csv"
CONTRATOS = "bases/contratos_consolidado_real.xlsx"
RESOLUCION = "1778"

# --- EL MÉTODO SE PONE AQUÍ (FUERA DEL MAIN) ---
def configurar_driver(ruta_descarga):
    ruta_driver = r"msedgedriver.exe"
    options = webdriver.EdgeOptions()
    ruta_abs = os.path.abspath(ruta_descarga)

    # IMPORTANTE: Cambia esta ruta si tu carpeta de usuario tiene otro nombre
    perfil_bot = r"C:\Users\Esteban Getial\AppData\Local\Microsoft\Edge\User Data Bot"

    # Esto usa tu perfil real (sesión iniciada, historial, etc.)
    options.add_argument(f"user-data-dir={perfil_bot}")
    options.add_argument("profile-directory=Default")

    # Camuflaje para evitar el "No soy un robot"
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    prefs = {
        "download.default_directory": ruta_abs,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Edge(service=Service(executable_path=ruta_driver), options=options)

    # Elimina la bandera de "webdriver" que detectan los sitios
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def descargar_contratos():
    # 1. Carga de bases
    base_dir = "873"
    contratos_x_resolucion = pd.read_excel("bases/CONTRATOS_TODAS_RES_MAR_2026.xlsx")
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260327.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].apply(limpiar_numero_contrato)
    contratacion_juridica_2024 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2024")
    contratacion_juridica_2025 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2025")
    contratacion_juridica_2026 = pd.read_excel("bases/BASE CONTRATACIÓN 2026.xlsx", sheet_name="2026")

    contratacion_juridica = pd.concat(
        [contratacion_juridica_2024, contratacion_juridica_2025, contratacion_juridica_2026], ignore_index=True)

    df_contratos = contratos_x_resolucion.merge(secop, left_on="1.7", right_on="Referencia del Proceso", how="left")
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()].drop_duplicates(["1.7", "URLProceso"])[
        ['1.7', 'URLProceso', '1.2']]
    df_verificar_manualmente = df_contratos[df_contratos["URLProceso"].isna()].drop_duplicates(["1.7", "URLProceso"])

    df_por_descargar["1.2"] = df_por_descargar["1.2"].astype(str).str.strip()

    df_por_descargar = df_por_descargar[df_por_descargar["1.2"] == base_dir]

    # Ajusta aquí cuántos contratos quieres procesar
    start_idx = 400
    end_idx = 600
    df_test = df_por_descargar.iloc[start_idx:end_idx]
    # df_test = df_por_descargar
    # df_test = df_por_descargar[df_por_descargar["1.7"].isin([
    # Bloque inicial y serie 160
    # "081-2026", "082-2026", "162-2026", "163-2026", "164-2026",
    #
    # # Bloque de la serie 300 (Primera parte)
    # "325-2026", "326-2026", "327-2026", "328-2026", "329-2026",
    # "330-2026", "331-2026", "332-2026", "333-2026", "334-2026",
    # "336-2026", "338-2026", "339-2026", "341-2026", "342-2026",
    # "343-2026", "345-2026", "346-2026", "347-2026", "348-2026",
    # "349-2026", "350-2026", "351-2026", "352-2026", "353-2026",
    # "354-2026", "355-2026", "356-2026", "357-2026", "358-2026",
    # "359-2026",
    #
    # # Bloque de la serie 300 (Segunda parte)
    # "360-2026", "361-2026", "362-2026", "363-2026", "364-2026",
    # "365-2026", "366-2026", "367-2026", "368-2026", "369-2026",
    # #"370-2026", "371-2026", "372-2026", "375-2026", "376-2026",
    # #"377-2026", "379-2026"
    # #])]

    driver = configurar_driver(base_dir)
    try:
        for index, fila in df_test.iterrows():
            url_proceso = fila["URLProceso"]
            num_contrato = str(fila["1.7"]).replace("/", "-").strip()

            # Definir la ruta absoluta para que Chrome/Edge no se confunda
            ruta_especifica = os.path.abspath(os.path.join(str(base_dir), num_contrato))
            if not os.path.exists(ruta_especifica):
                os.makedirs(ruta_especifica)

            # 2. CAMBIO DINÁMICO DE CARPETA (Sin cerrar el navegador)
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": ruta_especifica
            })

            print(f"\n📂 Procesando Contrato: {num_contrato}")

            try:
                driver.get(url_proceso)

                if index == start_idx:
                    print("👉 Verifica si el SECOP cargó bien y presiona ENTER...")
                    input()

                time.sleep(random.uniform(5, 8))  # Pausa aleatoria

                # Scroll simulado
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, 0);")

                contenedor_docs = driver.find_element(By.ID, "fdsContractDocumentsFieldset_tblDetail")
                enlaces = contenedor_docs.find_elements(By.XPATH, ".//a[contains(@onclick, 'DownloadFile')]")

                for enlace in enlaces:
                    try:
                        celda_nombre = enlace.find_element(By.XPATH, "./parent::td/preceding-sibling::td[1]")
                        nombre_archivo = celda_nombre.find_element(By.TAG_NAME, "span").text.strip()

                        driver.execute_script("arguments[0].click();", enlace)
                        print(f"   📥 Descargando: {nombre_archivo}")
                        time.sleep(random.uniform(4.0, 6.0))

                    except Exception as e:
                        print(f"   ❌ Error en archivo: {e}")

            except Exception as e:
                print(f"❌ Error en contrato {num_contrato}: {e}")

            # ELIMINAMOS EL driver.quit() DE AQUÍ
            print(f"✅ Contrato {num_contrato} finalizado.")

    finally:
        # 3. CERRAR EL DRIVER SOLO CUANDO TERMINE TODO EL BUCLE
        print("\n🏁 Proceso terminado. Cerrando navegador...")
        driver.quit()
        print(f"Buscar manualmente: {fila['1.7']} - {fila['URLProceso']}\n")


def extraer_texto_pdf(ruta_pdf: str, num_pages) -> str:
    """
    Extrae texto:
    - Si `num_pages` es truthy → busca en todas las páginas.
    - Si `num_pages` es falsy → solo en la primera página.
    """
    doc = fitz.open(ruta_pdf)
    textos = []

    # Si num_pages es truthy, iterar todas las páginas; si no, solo la primera
    pages_to_check = range(len(doc)) if num_pages else [0]

    for idx in pages_to_check:
        pagina = doc[idx]
        texto_pagina = pagina.get_text().strip()

        if texto_pagina:
            textos.append(texto_pagina)
            continue

        # Página sin texto → OCR
        matriz = fitz.Matrix(2, 2)
        pixmap = pagina.get_pixmap(matrix=matriz)
        imagen = Image.open(io.BytesIO(pixmap.tobytes("png")))
        texto_ocr = pytesseract.image_to_string(imagen, lang="spa+eng").strip()

        if texto_ocr:
            textos.append(texto_ocr)

    doc.close()
    return "\n\n".join(textos).strip()


def contiene_keyword(texto: str, keywords: list) -> tuple:
    """
    Busca la keyword en el texto.
    Solo verifica mayúsculas sobre la keyword encontrada, no la línea completa.
    """
    import unicodedata
    import re

    def normalizar(t):
        t = t.upper()
        t = unicodedata.normalize("NFD", t)
        t = "".join(c for c in t if unicodedata.category(c) != "Mn")
        return t

    for keyword, prefijo in keywords:
        # Buscamos la keyword SIN normalizar para verificar mayúsculas
        patron = re.compile(re.escape(normalizar(keyword)))

        for match in patron.finditer(normalizar(texto)):
            # Extraemos el fragmento ORIGINAL en las mismas posiciones
            fragmento_original = texto[match.start():match.end()]

            # ← Solo verificamos mayúsculas sobre ese fragmento exacto
            if any(c.islower() for c in fragmento_original):
                continue  # esa coincidencia es minúscula → ignorar

            return True, prefijo  # ← fragmento en mayúsculas → válido

    return False, None


def renombrar_archivo(ruta_original: str, prefijo: str) -> str:
    """
    Renombra el archivo agregando el prefijo al nombre original.
    Ejemplo: 'doc_001.pdf' → 'CONTRATO_doc_001.pdf'
    Retorna la nueva ruta.
    """
    carpeta = os.path.dirname(ruta_original)
    nuevo_nombre = f"{prefijo}"
    nueva_ruta = os.path.join(carpeta, nuevo_nombre)

    os.rename(ruta_original, nueva_ruta)
    return nueva_ruta


def listar_contratos():
    contratos_x_resolucion = pd.read_excel(CONTRATOS, sheet_name=RESOLUCION)
    secop = pd.read_csv(BASE_SECOP)
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].str.strip()

    # df_por_descargar = df_por_descargar[df_por_descargar["1.7"].isin(
    # ["199-2024", "219-2024", "260-2024", "262-2025", "191-2024", "184-2024", "186-2024"])]

    # df_por_descargar = df_por_descargar[0:3]

    resultados = {"renombrados": [], "sin_match": [], "errores": []}

    # necesito extaraslas rutas de cada contrato descargado

    for idx, row in contratos_x_resolucion.iterrows():
        # Obtén valores existentes (ajusta los nombres de columna si es necesario)
        contrato = row.get("numero_contrato")
        resolucion = row.get("resolucion")
        print(f"Contrato: {contrato} - Resolución: {resolucion}")
        # Ejemplo: agregar/actualizar campo 'procesado' y 'nota'


        # contratos_x_resolucion.at[idx, "nota"] = f"Procesado para revisión — {resolucion}"
        ruta_contrato = os.path.join("1778", str(contrato).replace("/", "-"))
        contratos_x_resolucion.at[idx, "contrato"] = buscar_en_ruta(ruta_contrato, contrato)

    contratos_x_resolucion.to_excel("bases/contratos_consolidado_real.xlsx", sheet_name=RESOLUCION, index=False)


def buscar_en_ruta(ruta_contrato, contrato):
    if os.path.exists(ruta_contrato):
        archivos = os.listdir(ruta_contrato)
        for archivo in archivos:
            try:
                texto = extraer_texto_pdf(ruta_contrato + "/" + archivo, num_pages=False)
                condicion, nombre = contiene_keyword(texto, KEYWORD)

                if condicion:
                    # nueva_ruta = renombrar_archivo(ruta_contrato + "/" + archivo, nombre + contrato + ".pdf")
                    print(f"  ✅ Renombrado ")
                    return True
                else:
                    print(f"  ⏭️  Sin match, se omite")

            except Exception as e:
                print(f"  ❌ Error: {e}")
        return False
    else:
        print(f"Contrato: {contrato} - Ruta: NO DESCARGADO")

    # for contrato in contratos_x_resolucion['numero_contrato']:
    #     ruta_contrato = os.path.join("1778", str(contrato).replace("/", "-"))
    #     if os.path.exists(ruta_contrato):
    #
    #         # lsitar archivos de la carpeta
    #         archivos = os.listdir(ruta_contrato)
    #         for archivo in archivos:
    #             print(f"Procesando: {contrato} - Archivo: {archivo}")
    #             try:
    #                 texto = extraer_texto_pdf(ruta_contrato + "/" + archivo)
    #                 condicion , nombre =  contiene_keyword(texto, KEYWORD)
    #                 if condicion:
    #                     nueva_ruta = renombrar_archivo(ruta_contrato + "/" + archivo, nombre + contrato + ".pdf")
    #                     print(f"  ✅ Renombrado → {os.path.basename(nueva_ruta)}")
    #                     resultados["renombrados"].append(nueva_ruta)
    #                 else:
    #                     print(f"  ⏭️  Sin match, se omite")
    #                     resultados["sin_match"].append(ruta_contrato + "/" + archivo)
    #
    #             except Exception as e:
    #                 print(f"  ❌ Error: {e}")
    #                 resultados["errores"].append(ruta_contrato + "/" + archivo)
    #
    #
    #
    #     else:
    #         print(f"Contrato: {contrato} - Ruta: NO DESCARGADO")


LIMITE_BYTES = 2 * 1024 * 1024  # 2MB
DPI_OBJETIVO = 150  # balance calidad/peso para docs escaneados
JPEG_CALIDAD = 75  # 0-100, 75 es buen balance


# 0-100, 75 es buen balance

def optimizar_pdf(ruta_entrada: str, ruta_salida: str) -> dict:
    """
    Comprime el PDF sin re-renderizar imágenes.
    - garbage=4 → elimina objetos huérfanos y duplicados
    - deflate=True → comprime streams de texto y metadatos
    - clean=True → limpia estructura interna
    Las imágenes quedan intactas → sin pérdida de calidad.
    """
    peso_original = os.path.getsize(ruta_entrada)

    doc = fitz.open(ruta_entrada)
    doc.save(
        ruta_salida,
        garbage=4,
        deflate=True,
        deflate_images=True,  # ← comprime imágenes con zlib sin pérdida
        deflate_fonts=True,  # ← comprime fuentes embebidas
        clean=True,
    )
    doc.close()

    peso_final = os.path.getsize(ruta_salida)
    ahorro = peso_original - peso_final
    ahorro_pct = (ahorro / peso_original * 100) if peso_original > 0 else 0

    return {
        "original_mb": round(peso_original / 1024 / 1024, 2),
        "final_mb": round(peso_final / 1024 / 1024, 2),
        "ahorro_pct": round(ahorro_pct, 1),
        "bajo_limite": peso_final <= LIMITE_BYTES,
    }


PREFIJOS_VALIDOS = (
    "CONTRATO_TIPO_1_",
    "CONTRATO_TIPO_2_",
    "CONTRATO_TIPO_3_",
    "CONTRATO_TIPO_4_",
    "CONTRATO_TIPO_5_",
    "CONTRATO_TIPO_7_",
    "CONTRATO_TIPO_8_",
    "CONTRATO_TIPO_9_",
)


def unificar_pdfs_carpeta(ruta_carpeta: str, numero_contrato: str) -> str | None:
    nombre_unificado = f"CONTRATO_UNIFICADO_{numero_contrato}.pdf"
    nombre_optimizado = f"CONTRATO_UNIFICADO_{numero_contrato}_FINAL.pdf"
    ruta_unificado = os.path.join(ruta_carpeta, nombre_unificado)
    ruta_optimizado = os.path.join(ruta_carpeta, nombre_optimizado)

    if os.path.exists(ruta_optimizado):
        print(f"  ⏭️  Ya existe → {nombre_optimizado}")
        return ruta_optimizado

    # ← Solo archivos que empiecen por CONTRATO_TIPO_X_
    archivos_pdf = sorted([
        f for f in os.listdir(ruta_carpeta)
        if f.lower().endswith(".pdf")
           and f.startswith(PREFIJOS_VALIDOS)  # ← tuple funciona directo con startswith
    ])

    if not archivos_pdf:
        print(f"  ⚠️  Sin archivos CONTRATO_TIPO_X_ en {ruta_carpeta}")
        return None

    print(f"  📎 Archivos a unificar: {len(archivos_pdf)}")
    for f in archivos_pdf:
        print(f"      → {f}")

    try:
        # Paso 1 — Unificar
        doc_unificado = fitz.open()
        for archivo in archivos_pdf:
            with fitz.open(os.path.join(ruta_carpeta, archivo)) as doc:
                doc_unificado.insert_pdf(doc)
        doc_unificado.save(ruta_unificado)
        doc_unificado.close()

        # Paso 2 — Optimizar
        # stats = optimizar_pdf(ruta_unificado, ruta_optimizado)
        # icono = "✅" if stats["bajo_limite"] else "⚠️"
        # print(f"  {icono} {stats['original_mb']}MB → {stats['final_mb']}MB "
        # f"(ahorro {stats['ahorro_pct']}%)")

        return ruta_optimizado

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None


def extraer_orden(ruta: str) -> tuple:
    """
    Extrae (año, numero) del nombre para ordenar correctamente.
    Ejemplo: CONTRATO_UNIFICADO_123-2024.pdf → (2024, 123)
    """
    nombre = os.path.basename(ruta)
    match = re.search(r'(\d+)-(\d{4})', nombre)  # busca NUMERO-AÑO
    if match:
        numero = int(match.group(1))
        anio = int(match.group(2))
        return (anio, numero)
    return (9999, 9999)  # si no parsea → va al final


def crear_pdf_maestro(carpeta_base: str, nombre_maestro: str = "TODOS_LOS_CONTRATOS_1778.pdf") -> str | None:
    ruta_maestro = os.path.join(carpeta_base, nombre_maestro)

    if os.path.exists(ruta_maestro):
        print(f"⏭️  Ya existe maestro → {nombre_maestro}")
        return ruta_maestro

    archivos_unificados = []
    for subcarpeta in sorted(os.listdir(carpeta_base)):
        ruta_sub = os.path.join(carpeta_base, subcarpeta)
        if not os.path.isdir(ruta_sub):
            continue

        for archivo in os.listdir(ruta_sub):
            if archivo.startswith("CONTRATO_UNIFICADO_") and archivo.endswith(".pdf"):
                archivos_unificados.append(os.path.join(ruta_sub, archivo))
                break

    if not archivos_unificados:
        print("⚠️  No se encontraron archivos CONTRATO_UNIFICADO_")
        return None

    # ← Ordenar por (año, número)
    archivos_unificados.sort(key=extraer_orden)

    print(f"\n📚 Creando PDF maestro con {len(archivos_unificados)} contratos...")
    print(f"   Orden: {[os.path.basename(f) for f in archivos_unificados[:5]]} ...")

    try:
        doc_maestro = fitz.open()

        for ruta_unificado in archivos_unificados:
            nombre = os.path.basename(ruta_unificado)
            carpeta = os.path.dirname(ruta_unificado)
            ruta_opt = os.path.join(carpeta, nombre.replace(".pdf", "_opt.pdf"))

            print(f"\n  📎 {nombre}")

            stats = optimizar_pdf(ruta_unificado, ruta_opt)
            print(f"     🗜️  {stats['original_mb']}MB → {stats['final_mb']}MB "
                  f"(ahorro {stats['ahorro_pct']}%)")

            with fitz.open(ruta_opt) as doc:
                doc_maestro.insert_pdf(doc)

        doc_maestro.save(
            ruta_maestro,
            garbage=4,
            deflate=True,
            clean=True,
        )
        doc_maestro.close()

        peso_mb = os.path.getsize(ruta_maestro) / 1024 / 1024
        print(f"\n✅ PDF maestro → {nombre_maestro} | {len(archivos_unificados)} contratos | {peso_mb:.1f} MB")
        return ruta_maestro

    except Exception as e:
        print(f"❌ Error creando maestro: {e}")
        return None


def unificar_contratos():
    contratos_x_resolucion = pd.read_excel(CONTRATOS, sheet_name=RESOLUCION)

    resultados = {"unificados": [], "saltados": [], "errores": []}

    for contrato in contratos_x_resolucion["numero_contrato"]:
        # Sanitizamos el nombre para que sea válido en Windows
        numero_limpio = str(contrato).replace("/", "-")
        ruta_contrato = os.path.join("1778", numero_limpio)

        if not os.path.exists(ruta_contrato):
            print(f"  ⚠️  No descargado: {contrato}")
            resultados["errores"].append(contrato)
            continue

        print(f"\n📁 Procesando: {contrato}")

        # ← Aquí va la lógica de unificación
        ruta_resultado = unificar_pdfs_carpeta(ruta_contrato, numero_limpio)

        if ruta_resultado:
            resultados["unificados"].append(ruta_resultado)
        else:
            resultados["errores"].append(contrato)

        print("\n── RESUMEN ──────────────────────────────")
        print(f"  ✅ Unificados : {len(resultados['unificados'])}")
        print(f"  ⏭️  Saltados  : {len(resultados['saltados'])}")
        print(f"  ❌ Errores   : {len(resultados['errores'])}")


DPI_MAESTRO = 85  # mínimo legible para documentos escaneados
JPEG_MAESTRO = 45  # agresivo pero texto aún legible


def optimizar_pdf_agresivo(ruta_entrada: str, ruta_salida: str) -> dict:
    """
    Re-renderiza cada página a 96 DPI y JPEG 45%.
    Objetivo: reducir ~65% del peso original.
    """
    peso_original = os.path.getsize(ruta_entrada)
    doc_original = fitz.open(ruta_entrada)
    doc_nuevo = fitz.open()

    matriz = fitz.Matrix(DPI_MAESTRO / 72, DPI_MAESTRO / 72)

    for num_pagina in range(len(doc_original)):
        pagina = doc_original[num_pagina]
        pixmap = pagina.get_pixmap(matrix=matriz, alpha=False)
        jpeg_bytes = pixmap.tobytes("jpeg", jpg_quality=JPEG_MAESTRO)
        pixmap = None

        nueva_pagina = doc_nuevo.new_page(
            width=pagina.rect.width,
            height=pagina.rect.height
        )
        nueva_pagina.insert_image(pagina.rect, stream=jpeg_bytes)

    doc_original.close()

    doc_nuevo.save(
        ruta_salida,
        garbage=4,
        deflate=True,
        clean=True,
    )
    doc_nuevo.close()

    peso_final = os.path.getsize(ruta_salida)
    ahorro_pct = ((peso_original - peso_final) / peso_original * 100) if peso_original > 0 else 0

    return {
        "original_mb": round(peso_original / 1024 / 1024, 2),
        "final_mb": round(peso_final / 1024 / 1024, 2),
        "ahorro_pct": round(ahorro_pct, 1),
    }


def crear_pdf_maestro_agreesivo(carpeta_base: str, nombre_maestro: str = "TODOS_LOS_CONTRATOS_1778.pdf",
                                nombre_a_unificar: str = "CONTRATO_UNIFICADO_") -> str | None:
    ruta_maestro = os.path.join(carpeta_base, nombre_maestro)
    ruta_sin_opt = os.path.join(carpeta_base, "MAESTRO_SIN_OPTIMIZAR.pdf")
    df_guia = pd.read_excel(CONTRATOS, sheet_name=RESOLUCION)
    # ── Paso 1: unir todos sin optimizar ────────────────────────
    archivos_unificados = []
    for subcarpeta in df_guia["numero_contrato"].apply(lambda x: str(x).replace("/", "-")):
        ruta_sub = os.path.join(carpeta_base, subcarpeta)
        if not os.path.isdir(ruta_sub):
            continue
        for archivo in os.listdir(ruta_sub):
            if archivo.startswith(nombre_a_unificar) and archivo.endswith(".pdf"):
                archivos_unificados.append(os.path.join(ruta_sub, archivo))
                # break

    if not archivos_unificados:
        print(f"⚠️  No se encontraron archivos {nombre_a_unificar}")
        return None

    archivos_unificados.sort(key=extraer_orden)

    print(f"\n📚 Uniendo {len(archivos_unificados)} contratos...")
    doc_maestro = fitz.open()
    for ruta in archivos_unificados:
        print(f"  📎 {os.path.basename(ruta)}")
        with fitz.open(ruta) as doc:
            doc_maestro.insert_pdf(doc)

    doc_maestro.save(ruta_sin_opt, garbage=4, deflate=True, clean=True)
    doc_maestro.close()

    peso_sin_opt = os.path.getsize(ruta_sin_opt) / 1024 / 1024
    print(f"\n  📊 Sin optimizar: {peso_sin_opt:.1f}MB")

    # ── Paso 2: optimizar agresivo ───────────────────────────────
    print(f"\n🗜️  Optimizando a 96 DPI / JPEG 45%...")
    stats = optimizar_pdf_agresivo(ruta_sin_opt, ruta_maestro)

    print(f"\n✅ PDF maestro → {nombre_maestro}")
    print(f"   📊 {stats['original_mb']}MB → {stats['final_mb']}MB (ahorro {stats['ahorro_pct']}%)")
    print(f"   {'✅ Bajo 500MB' if stats['final_mb'] <= 500 else '⚠️  Aún sobre 500MB — bajar JPEG_MAESTRO a 35'}")

    # Limpiar temporal
    if os.path.exists(ruta_sin_opt):
        os.remove(ruta_sin_opt)

    return ruta_maestro


def rectificar_contratos():
    contratos_x_resolucion = pd.read_excel("bases/CONTRATOS_1778.xlsx")
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260312.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].str.strip()

    df_contratos = contratos_x_resolucion.merge(secop, left_on="1.7", right_on="Referencia del Proceso", how="left")
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()].drop_duplicates(["1.7", "URLProceso"])[
        ['1.7', 'URLProceso']]

    df_verificar_manualmente = df_contratos[df_contratos["URLProceso"].isna()].drop_duplicates(["1.7", "URLProceso"])

    for contrato in df_por_descargar["1.7"]:
        ruta_contrato = os.path.join("1778", str(contrato).replace("/", "-"))
        for archivo in os.listdir(ruta_contrato):
            print(f" {contrato} | {archivo}")
        # if not os.path.exists(ruta_contrato):
        # print(f"Contrato: {contrato} - Ruta: NO DESCARGADO")

    # for contrato in df_verificar_manualmente["1.7"]:
    # print(f"Contrato: {contrato} - URL: {df_verificar_manualmente[df_verificar_manualmente['1.7'] == contrato]['URLProceso'].values[0]}")


def listar_certificados():
    resultados = {"renombrados": [], "sin_match": [], "errores": []}
    KEYWORD = [
        ("CERTIFICADO DE SUPERVICION O INTERVENTORIA", "SUPERVISION_1_"),
        ("CERTIFICADO DE SUPERVISIÓN O INTERVENTORÍA", "SUPERVISION_3_"),
        ("GTH-CSI 011", "SUPERVISION_2_"),
    ]

    contratos_x_resolucion = pd.read_excel("bases/CONTRATOS_1778.xlsx")
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260312.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].str.strip()

    df_contratos = contratos_x_resolucion.merge(
        secop, left_on="1.7", right_on="Referencia del Proceso", how="left"
    )
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()] \
        .drop_duplicates(["1.7", "URLProceso"])[['1.7', 'URLProceso']]

    for contrato in df_por_descargar["1.7"]:
        ruta_contrato = os.path.join("1778", str(contrato).replace("/", "-"))
        contrato_limpio = str(contrato).replace("/", "-")

        if not os.path.exists(ruta_contrato):
            print(f"Contrato: {contrato} - Ruta: NO DESCARGADO")
            continue

        consecutivo_acta = 0  # ← reinicia por carpeta

        for archivo in os.listdir(ruta_contrato):
            print(f"Procesando: {contrato} - Archivo: {archivo}")

            if archivo == f"CONTRATO_UNIFICADO_{contrato_limpio}.pdf":
                continue
            if archivo.startswith("CONTRATO_TIPO_") and archivo.endswith(".pdf"):
                continue
            if archivo.startswith("DOC-PRECONTRACTUALES") and archivo.endswith(".pdf"):
                continue

            try:
                ruta_archivo = os.path.join(ruta_contrato, archivo)
                paginas = extraer_texto_pdf_con_indice(ruta_archivo, num_pages=True)
                condicion, prefijo, num_pagina = contiene_keyword_indice(paginas, KEYWORD)

                if condicion:
                    consecutivo_acta += 1

                    # ← SIN renombrar el archivo original

                    # Solo extraer la página donde está la keyword
                    nombre_acta = f"ACTA_{consecutivo_acta}_{contrato_limpio}.pdf"
                    ruta_acta = os.path.join(ruta_contrato, nombre_acta)

                    doc_orig = fitz.open(ruta_archivo)
                    doc_acta = fitz.open()
                    doc_acta.insert_pdf(doc_orig, from_page=num_pagina, to_page=num_pagina)
                    doc_acta.save(ruta_acta, garbage=4, deflate=True, clean=True)
                    doc_acta.close()
                    doc_orig.close()

                    print(f"  📄 Acta extraída → {nombre_acta} (página {num_pagina + 1})")
                    resultados["renombrados"].append(ruta_acta)
                else:
                    print(f"  ⏭️  Sin match")
                    resultados["sin_match"].append(ruta_archivo)

            except Exception as e:
                print(f"  ❌ Error: {e}")
                resultados["errores"].append(os.path.join(ruta_contrato, archivo))

    print("\n── RESUMEN ──────────────────────────────")
    print(f"  ✅ Renombrados : {len(resultados['renombrados'])}")
    print(f"  ⏭️  Sin match  : {len(resultados['sin_match'])}")
    print(f"  ❌ Errores    : {len(resultados['errores'])}")


if __name__ == "__main__":
    # descargar_contratos()

    ## Se encarga de revisar cada contrato descargado, extraer el texto, buscar las keywords y renombrar los archivos
    #listar_contratos()

    # verificar ucales contratos faltan
    #rectificar_contratos()

    # Se encarga de obtener el contratos y unirlos en uno solo
    #unificar_contratos()

    # Paso 2 — PDF maestro con todos
    crear_pdf_maestro_agreesivo(carpeta_base=RESOLUCION)

    # Paso 4 Unificar certificado de supervision
    # listar_certificados()

    # Paso 5 Unificar certificados de supervision
    # crear_pdf_maestro_agreesivo(carpeta_base="1778", nombre_maestro="SER124SREC20260331NI000900091143ID2177823635D03.pdf", nombre_a_unificar="ACTA_")

    # se erncarga de unificar los contratos en un solo PDF por contrato, para facilitar su lectura y análisis posterior ademas de bajar el peso de los archivos.
