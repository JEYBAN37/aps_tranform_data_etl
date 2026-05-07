import os
from tkinter import Tk, filedialog, messagebox

import pandas as pd
import win32com
from PIL import Image
import win32com.client

from automatizacion_looker_aps.contratacion_cruzada.EscanerAPS.app_digitalizaion import AppDigitalizacion

CARPETA_BASE ='F:/APS AUTOMATIZACIONES/aps/automatizacion_looker_aps/contratacion_cruzada'





def seleccionar_carpeta_raiz():
    """Abre una ventana para elegir la carpeta de Contratación Cruzada."""
    root = Tk()
    root.withdraw() # Oculta la ventana pequeña de tkinter
    root.attributes('-topmost', True) # Pone la ventana al frente
    print("📂 Selecciona la carpeta principal 'contratacion_cruzada'...")
    carpeta = filedialog.askdirectory(title="Seleccione la carpeta de Contratación Cruzada")
    root.destroy()
    return carpeta

def escaneo(nombre_documento, sufijo, ruta_contrato ,threshold=128):
    # 1. Llamar al escáner mediante Windows WIA
    wia = win32com.client.Dispatch("WIA.CommonDialog")
    # Abre la interfaz de Windows para escanear
    img_file = wia.ShowAcquireImage()

    if img_file is None:
        print("Escaneo cancelado por el usuario.")
        return None

    # 2. Guardar temporalmente la imagen del escáner
    temp_jpg = os.path.join(ruta_contrato, "temp_scan.jpg")
    if os.path.exists(temp_jpg): os.remove(temp_jpg)  # Limpiar si existe
    img_file.SaveFile(temp_jpg)

    # 3. Procesar la imagen con PIL (Igual que hacías con OpenCV)
    # Abrimos, convertimos a escala de grises y aplicamos el umbral (B&W)
    pil_img = Image.open(temp_jpg).convert("L")

    # Binarización (Equivalente al threshold que hacías antes)
    bw = pil_img.point(lambda p: 255 if p > threshold else 0, mode="1")

    # 4. Convertir a PDF y guardar
    pdf_path = os.path.join(ruta_contrato, f"{nombre_documento}_{sufijo}.pdf")

    # El backend de PDF prefiere RGB para evitar errores de compatibilidad
    bw.convert("RGB").save(pdf_path, "PDF", resolution=300.0)

    print(f"✅ Escaneado con éxito y guardado en: {pdf_path}")

    # 5. Limpieza del archivo temporal
    os.remove(temp_jpg)

def escanear_actas( df_actas , ruta_contrato):
    print("Escaneando ACTAS...")
    # Aquí iría la lógica para escanear las actas
    found = False
    numero_acta = None
    while not found:
        numero_acta = str(input("Digita los numeros del acta "))
        if numero_acta in df_actas["orden"].values:
            print("Acta encontrada")
            found = True
        else:
            print("Acta no encontrada, intenta de nuevo")

    print(f"Iniciando escaneo para Acta: {numero_acta}...")

    # Asegurar que la carpeta del contrato existe
    os.makedirs(ruta_contrato, exist_ok=True)

    try:
    # Llamar a la función de escaneo
        escaneo(nombre_documento="ACTA", sufijo=numero_acta, ruta_contrato=ruta_contrato)
        return numero_acta

    except Exception as e:
        print(f"❌ Error durante el proceso de escaneo: {e}")
        return None

def escanear_supervision():
    print("Escaneando SUPERVISION...")
    # Aquí iría la lógica para escanear las supervisiones
    pass


def limpiar_pantalla():
    """Limpia la consola enviando comandos de escape y saltos de línea."""
    if os.name == 'nt':
        # Intenta el comando de Windows
        os.system('cls')
    else:
        # Intenta el comando de Unix/Linux/Mac
        os.system('clear')

    # El truco maestro:
    # \033[H mueve el cursor al inicio, \033[2J limpia la pantalla
    print("\033[H\033[2J", end="")
    # Por si acaso la terminal no soporta ANSI, enviamos saltos de línea
    print("\n" * 50)






