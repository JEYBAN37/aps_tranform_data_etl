"""
Clasifica cada página de un PDF (orden_pago / informe_supervision)
y la extrae como archivo individual en la carpeta de su categoría.

Pipeline optimizado:
  1. Lee el texto plano de todas las páginas de una vez.
  2. Aplica reglas cruzadas (Si una es orden_pago, la otra por descarte es supervisión).
  3. Si quedan dudas o el texto viene vacío (escaneado), va a Gemini 2.5 Flash en un solo bloque.
"""
import gc
import os
import re
import json
import hashlib
import requests
import pandas as pd
import unicodedata
import time
import fitz  # PyMuPDF
from pydantic import BaseModel, Field
from google import genai

# ── CONFIGURACIÓN ────────────────────────────────────────────────
API_KEY = "GCP_API_KEY"
client = genai.Client(api_key=API_KEY)
MODELO = "gemini-2.5-flash"  # Ultra rápido y estable

CARPETA_SALIDA_BASE = "clasificados"
CARPETAS_CATEGORIA = {
    "orden_pago": os.path.join(CARPETA_SALIDA_BASE, "orden_pago"),
    "informe_supervision": os.path.join(CARPETA_SALIDA_BASE, "informe_supervision"),
}
CARPETA_REVISAR = os.path.join(CARPETA_SALIDA_BASE, "revisar")

REGLAS_EXACTAS = [
    ("ORDEN DE PAGO N", "orden_pago"),
    ("CERTIFICADO DE SUPERVISIÓN O INTERVENTORÍA", "informe_supervision"),
    ("GTH-CSI", "informe_supervision"),
]

UMBRAL_CONFIANZA_IA = 0.85


class AnalisisDocumento(BaseModel):
    categoria: str = Field(description="Debe ser 'orden_pago', 'informe_supervision' o 'ninguna'")
    confianza: float = Field(description="Nivel de certeza de 0.0 a 1.0")
    orden_pago_numero: str = Field(default="", description="Si es orden_pago, extrae el número correlativo numérico.")
    numero_contrato: str = Field(default="",
                                 description="Si encuentras el número de contrato (ej: 492-2024), extráelo.")
    razon: str = Field(description="Explicación breve")


# ─────────────────────────────────────────────────────────────────
#  UTILIDADES
# ─────────────────────────────────────────────────────────────────

def normalizar(t: str) -> str:
    t = t.upper()
    t = unicodedata.normalize("NFD", t)
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def hash_corto(texto: str) -> str:
    return hashlib.md5(texto.encode("utf-8")).hexdigest()[:4]


def asegurar_carpetas():
    for carpeta in CARPETAS_CATEGORIA.values():
        os.makedirs(carpeta, exist_ok=True)
    os.makedirs(CARPETA_REVISAR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────
#  LLAMADA CON IA (CON BACKOFF / REINTENTOS)
# ─────────────────────────────────────────────────────────────────

def clasificar_con_ia(ruta_pagina_pdf: str) -> dict:
    documento_pdf = client.files.upload(file=ruta_pagina_pdf)

    prompt = """
    Analiza esta página de un documento de salud. Clasifícala en el JSON solicitado.
    - 'orden_pago': Tiene datos presupuestales, rubros, Neto a Pagar.
    - 'informe_supervision': Certificación de cumplimiento del supervisor (ej: GTH-CSI).
    """

    intentos_maximos = 3
    segundos_espera = 5

    for intento in range(intentos_maximos):
        try:
            respuesta = client.models.generate_content(
                model=MODELO,
                contents=[documento_pdf, prompt],
                config={"response_mime_type": "application/json", "response_schema": AnalisisDocumento}
            )
            data = json.loads(respuesta.text)
            data["fuente"] = "ia"
            return data
        except Exception as e:
            if "503" in str(e) and intento < intentos_maximos - 1:
                print(f"  ⏳ Servidor ocupado. Reintentando en {segundos_espera}s... ({intento + 1}/{intentos_maximos})")
                time.sleep(segundos_espera)
                continue
            else:
                print(f"  ❌ Error de IA: {e}")
                return {"categoria": "none", "confianza": 0.0, "orden_pago_numero": "", "numero_contrato": "",
                        "fuente": "error"}
        finally:
            if intento == intentos_maximos - 1 or 'data' in locals():
                try:
                    client.files.delete(name=documento_pdf.name)
                except:
                    pass


# ─────────────────────────────────────────────────────────────────
#  MARCAR EN SHHETS TRUE A LA ORDEN DE PAGO CORRESPONDIENTE
# ─────────────────────────────────────────────────────────────────

def marcar_orden_pago_en_sheets(numero_orden_pago: str):
    ejecutable_script = "AKfycbybbDArDM06_8wegb7D7iVDGhrLC5rw79kl-0PJ-YeA6KyIW2BlzaLcJrcinurm1SRx"
    url_script = f"https://script.google.com/macros/s/{ejecutable_script}/exec"

    try:
        params = {'acta': numero_orden_pago}
        # allow_redirects es obligatorio para Google Apps Script
        respuesta = requests.get(url_script, params=params, allow_redirects=True)

        if "Éxito" in respuesta.text:
            print(f"✅ El script marcó correctamente el acta {numero_orden_pago} en la hoja de pagos.")
        else:
            print(f"Advertencia: El script no encontró el acta {numero_orden_pago}")
    except Exception as e:
        print(f"Error de conexión con la nube: {e}")


# ─────────────────────────────────────────────────────────────────
#  PROCESAMIENTO OPTIMIZADO
# ─────────────────────────────────────────────────────────────────

def procesar_pdf(ruta_pdf: str) -> dict:
    asegurar_carpetas()
    resultados = {"orden_pago": [], "informe_supervision": [], "revisar": []}

    doc = fitz.open(ruta_pdf)
    nombre_base_pdf = os.path.splitext(os.path.basename(ruta_pdf))[0]
    total_paginas = len(doc)
    print(f"\n📄 {nombre_base_pdf}.pdf — {total_paginas} página(s)")

    clasificaciones = {}
    numero_orden_pago_global = None

    # --- PASADA 1: Extracción de texto plano y Reglas Locales Gratis ---
    for num_pagina in range(total_paginas):
        page = doc[num_pagina]
        texto_extraido = page.get_text()
        texto_normalizado = normalizar(texto_extraido)

        categoria_detectada = None
        for keyword, cat in REGLAS_EXACTAS:
            if normalizar(keyword) in texto_normalizado:
                categoria_detectada = cat
                break

        # Guardar pre-análisis por regla
        if categoria_detectada:
            num_contrato = "SN"
            match_c = re.search(r'\b(\d{1,5}-\d{4})\b', texto_extraido)
            if match_c: num_contrato = match_c.group(1)

            num_op = ""
            match_op = re.search(r'ORDEN DE PAGO N[°ºO\.:]*\s*:?\s*(\d+)', texto_extraido, re.IGNORECASE)
            if match_op: num_op = match_op.group(1)

            clasificaciones[num_pagina] = {
                "texto": texto_extraido,
                "categoria": categoria_detectada,
                "confianza": 1.0,
                "orden_pago_numero": num_op,
                "numero_contrato": num_contrato,
                "fuente": "regla"
            }
            if cat == "orden_pago" and num_op:
                numero_orden_pago_global = num_op
        else:
            clasificaciones[num_pagina] = {"texto": texto_extraido, "fuente": "pendiente"}

    # --- LÓGICA DE DESCARTE (Si vienen exactamente 2 páginas) ---
    if total_paginas == 2:
        pag_0_fuente = clasificaciones[0]["fuente"]
        pag_1_fuente = clasificaciones[1]["fuente"]

        # Caso A: Página 1 es Acta por regla, Página 2 no se sabe (viene escaneada)
        if pag_0_fuente == "regla" and pag_1_fuente == "pendiente":
            if clasificaciones[0]["categoria"] == "orden_pago":
                print(
                    "  💡 Deducción automática: Página 1 es 'orden_pago', por ende Página 2 es 'informe_supervision' (Gratis).")
                clasificaciones[1].update({
                    "categoria": "informe_supervision", "confianza": 1.0, "fuente": "deduccion_logica",
                    "orden_pago_numero": "", "numero_contrato": clasificaciones[0]["numero_contrato"]
                })

        # Caso B: Página 2 es Acta por regla, Página 1 no se sabe
        elif pag_1_fuente == "regla" and pag_0_fuente == "pendiente":
            if clasificaciones[1]["categoria"] == "orden_pago":
                print(
                    "  💡 Deducción automática: Página 2 es 'orden_pago', por ende Página 1 es 'informe_supervision' (Gratis).")
                clasificaciones[0].update({
                    "categoria": "informe_supervision", "confianza": 1.0, "fuente": "deduccion_logica",
                    "orden_pago_numero": "", "numero_contrato": clasificaciones[1]["numero_contrato"]
                })

    # --- PASADA 2: IA solo para lo que quede verdaderamente pendiente ---
    for num_pagina in range(total_paginas):
        if clasificaciones[num_pagina]["fuente"] == "pendiente":
            texto_temp = clasificaciones[num_pagina]["texto"]
            ruta_temporal = f"temp_pag_{num_pagina}_{hash_corto(texto_temp)}.pdf"

            doc_temporal = fitz.open()
            doc_temporal.insert_pdf(doc, from_page=num_pagina, to_page=num_pagina)
            doc_temporal.save(ruta_temporal)
            doc_temporal.close()

            resultado_ia = clasificar_con_ia(ruta_temporal)
            clasificaciones[num_pagina].update(resultado_ia)

            if os.path.exists(ruta_temporal):
                os.remove(ruta_temporal)

            if resultado_ia.get("categoria") == "orden_pago" and resultado_ia.get("orden_pago_numero"):
                numero_orden_pago_global = resultado_ia["orden_pago_numero"]

    # --- PASADA 3: Separación física y guardado final ---
    for num_pagina in range(total_paginas):
        info = clasificaciones[num_pagina]
        texto = info["texto"]
        categoria = info.get("categoria")
        confianza = info.get("confianza", 0.0)
        fuente = info.get("fuente")
        num_contrato = info.get("numero_contrato", "SN") or "SN"

        if categoria not in ["orden_pago", "informe_supervision"] or (
                fuente == "ia" and confianza < UMBRAL_CONFIANZA_IA):
            nombre_final = f"REVISAR_{nombre_base_pdf}_pag_{num_pagina + 1}.pdf"
            ruta_salida = os.path.join(CARPETA_REVISAR, nombre_final)
            print(f"  ⚠️ Página {num_pagina + 1}: Requiere revisión humana ({fuente})")
            resultados["revisar"].append(ruta_salida)
        else:
            h = hash_corto(texto)
            if categoria == "orden_pago":
                # Se nombra como ACTA_ + el número de la orden de pago
                id_op = numero_orden_pago_global or info.get("orden_pago_numero") or h
                nombre_final = f"ACTA_{id_op}.pdf"
                marcar_orden_pago_en_sheets(id_op)
            else:
                # Se nombra como SUPERVISION_ + contrato + hash corto
                nombre_final = f"SUPERVISION_{num_contrato}_{h}.pdf"

            ruta_salida = os.path.join(CARPETAS_CATEGORIA[categoria], nombre_final)
            icono = "✅" if "regla" in fuente or "deduccion" in fuente else "🤖"
            print(f"  {icono} Página {num_pagina + 1}: {categoria} [{fuente}] ({confianza:.0%}) → {nombre_final}")
            resultados[categoria].append(ruta_salida)

        # Recortar y guardar la página real



        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=num_pagina, to_page=num_pagina)
        doc_pagina.save(ruta_salida, garbage=4, deflate=True, clean=True)
        doc_pagina.close()

    doc.close()
    return resultados

def listar_actas_en_carpeta(carpeta: str) -> pd.DataFrame:
    """
    Lista todos los archivos PDF en la carpeta dada y devuelve un DataFrame con columnas:
    - 'ruta': ruta completa del archivo
    - 'nombre': nombre del archivo
    - 'categoria': 'orden_pago', 'informe_supervision' o 'revisar'
    """
    registros = []
    contratos = []
    for archivo in os.listdir(carpeta):
       nombre = archivo.split(" ")[3]
       nombre = nombre.replace("-1", "")
       nombre = nombre.replace(".pdf", "")
       contratos.append(f"{nombre}-2025")

    # cuenta cuantas veces aparece cada contrato
    conteo_contratos = pd.Series(contratos).value_counts().to_dict()
    df_contratos_conteo = pd.DataFrame(list(conteo_contratos.items()), columns=['contrato', 'conteo'])

    # 2. Abrir el archivo por su ID
    url_base_datos = "1WnG6EBin4IxEwhcg0aOtC0efh9k_fg7dWVXrV86ZUfY"
    # 3. Seleccionar la hoja por su nombre
    nombre_de_tu_hoja = "PAGADOS_REPORTE"  # Cambia esto por el nombre real de la pestaña


    # 4. Convertir a DataFrame
    df_contratos_en_online_reportados = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{url_base_datos}/gviz/tq?tqx=out:csv&sheet={nombre_de_tu_hoja}")


    df_contratos = df_contratos_en_online_reportados[df_contratos_en_online_reportados['resolucion'] == "ID2087325712"]

    df_merge = df_contratos.merge(df_contratos_conteo, left_on='numero_contrato', right_on='contrato', how='left').fillna(0)

    # calcula la diferencia de reportados_m y conteo
    df_merge['diferencia'] = df_merge['escaneados_m'] - df_merge['conteo']

    df_para_enviar = df_merge[df_merge['diferencia'] == 0]

    df_para_enviar.to_csv("contratos_para_enviar.csv", index=False)

    print(f"📊 Conteo de contratos en la carpeta '{carpeta}':")

if __name__ == "__main__":
    listar_actas_en_carpeta(r"H:\APS RES 873 JULIO\RESOLUCION 873 2025 - 2026\5. Actas de ejecución de los recursos parciales y finales suscritas por el supervisor o interventor. (Acta de pago mensual por talento humano, transporte)\5.1\ACTAS")
    #procesar_pdf("E:/CTO N492.pdf")