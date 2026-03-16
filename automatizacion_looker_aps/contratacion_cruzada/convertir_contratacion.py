import os
import random
import re
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service


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
    contratos_x_resolucion = pd.read_excel("bases/CONTRATOS_1778.xlsx")
    secop = pd.read_csv("bases/SECOP_II_-_Procesos_de_Contratación_20260312.csv")
    secop["Referencia del Proceso"] = secop["Referencia del Proceso"].str.strip()

    df_contratos = contratos_x_resolucion.merge(secop, left_on="1.6", right_on="Referencia del Proceso", how="left")
    df_por_descargar = df_contratos[df_contratos["URLProceso"].notna()].drop_duplicates(["1.6", "URLProceso"])[['1.6','URLProceso']]
    df_verificar_manualmente = df_contratos[df_contratos["URLProceso"].isna()].drop_duplicates(["1.6", "URLProceso"])

    # Ajusta aquí cuántos contratos quieres procesar
    start_idx = 200
    end_idx = 308
    df_test = df_por_descargar.iloc[start_idx:end_idx]

    base_dir = "1778"
    driver = configurar_driver(base_dir)
    try:
        for index, fila in df_test.iterrows():
            url_proceso = fila["URLProceso"]
            num_contrato = str(fila["1.6"]).replace("/", "-").strip()

            # Definir la ruta absoluta para que Chrome/Edge no se confunda
            ruta_especifica = os.path.abspath(os.path.join(base_dir, num_contrato))
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
        print(f"Buscar manualmente: {fila['1.6']} - {fila['URLProceso']}\n")




if __name__ == "__main__":
    descargar_contratos()