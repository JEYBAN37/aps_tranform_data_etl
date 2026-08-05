import os  # <- CORREGIDO: Faltaba importar os
import random
import re
import time

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait


def limpiar_nombre_archivo(texto):
    texto_limpio = texto.upper().strip()
    texto_limpio = re.sub(r"[ /(),\-\.]+", "_", texto_limpio)
    return texto_limpio.strip("_")


def esperando_y_renombrar(carpeta, nuevo_nombre):
    time.sleep(2)
    finalizado = False
    intentos = 0

    while not finalizado and intentos < 20:
        archivos = os.listdir(carpeta)
        archivos_crdownload = [f for f in archivos if f.endswith(".crdownload")]

        if not archivos_crdownload:
            archivos_completos = [
                os.path.join(carpeta, f)
                for f in archivos
                if os.path.isfile(os.path.join(carpeta, f))
            ]
            if archivos_completos:
                ultimo_archivo = max(archivos_completos, key=os.path.getmtime)

                # Evitamos renombrar archivos ya procesados o el propio script
                if "ANEXO_" not in os.path.basename(ultimo_archivo):
                    extensio = os.path.splitext(ultimo_archivo)[1]
                    # Agregamos un prefijo para identificarlo en las siguientes pasadas
                    ruta_nueva = os.path.join(
                        carpeta, f"ANEXO_{nuevo_nombre}{extensio}"
                    )
                    os.rename(ultimo_archivo, ruta_nueva)
                    print(f" Saved: ANEXO_{nuevo_nombre}{extensio}")
                    finalizado = True
                    break
        time.sleep(1)
        intentos += 1


def iniciar_sesion(usuario, contrasenna):
    URL_LOGIN_SIA = "https://siaobserva.auditoria.gov.co/login.aspx"
    opciones = uc.ChromeOptions()
    opciones.add_argument("--disable-blink-features=AutomationControlled")

    carpeta_descargas = os.path.join(r"H:\APS RES 873 JULIO\RESOLUCION 873 2025 - 2026\3. Contratos o actos administrativos formalizados para la ejecución de los recursos\contratos_Res_900")
    if not os.path.exists(carpeta_descargas):
        os.makedirs(carpeta_descargas)

    prefs = {"download.default_directory": carpeta_descargas}
    opciones.add_experimental_option("prefs", prefs)

    print("Iniciando navegador indetectable con soporte de sesión...")
    driver = uc.Chrome(options=opciones)

    try:
        # PASO 0: INICIO DE SESIÓN (AUTENTICACIÓN)
        # ==========================================
        print("Abriendo página de autenticación...")
        driver.get(URL_LOGIN_SIA)
        time.sleep(random.uniform(3.0, 4.5))

        print("Ingresando credenciales de acceso...")
        # Localizar campo de usuario (Inspecciona con F12 si el ID es 'txtUsuario' o 'txtLogin')
        input_usuario = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[contains(@id, 'Usuario') or contains(@id, 'Login') or @type='text']"))
        )
        input_usuario.clear()
        input_usuario.send_keys(usuario)

        # Localizar campo de contraseña
        input_pass = driver.find_element(By.XPATH,
                                         "//input[@type='password' or contains(@id, 'Password') or contains(@id, 'Clave')]")
        input_pass.clear()
        input_pass.send_keys(contrasenna)
        time.shape = time.sleep(random.uniform(1.0, 2.0))

        # Hacer clic en el botón de Ingresar / Login
        print("Haciendo clic en Ingresar...")
        boton_ingresar = driver.find_element(By.XPATH,
                                             "//input[@type='submit' or contains(@id, 'btnIngresar') or contains(@id, 'btnLogin')]")
        boton_ingresar.click()

        # Esperar a que el portal cargue el panel principal tras autenticarse
        print("Esperando validación de credenciales...")
        time.sleep(random.uniform(4.0, 6.0))

    except Exception as e:
        print(f"Error durante la autenticación: {e}")
        driver.quit()
        raise



    return driver, carpeta_descargas


def descargar_soportes_sia(contratos, driver, carpeta_descargas):
    URL_BUSCADOR_SIA = "https://siaobserva.auditoria.gov.co/informe_cont_base.aspx"

    try:
        pestaña_principal = driver.current_window_handle

        for contrato in contratos:

            print(f"\n========================================================")
            print(f" INICIANDO PROCESO DE BÚSQUEDA PARA: {contrato}")
            print(f"========================================================")

            # Control de pestañas para no mezclar contratos
            if len(driver.window_handles) > 1:
                for ventana in driver.window_handles:
                    if ventana != pestaña_principal:
                        driver.switch_to.window(ventana)
                        driver.close()
                driver.switch_to.window(pestaña_principal)

            print("Redirigiendo al buscador de contratos...")
            driver.get(URL_BUSCADOR_SIA)
            time.sleep(random.uniform(3.0, 4.5))

            # ==========================================
            # PASO 3A Y 3B: FECHAS
            # ==========================================
            fecha_inicio = "01/01/2025"
            input_fecha_ini = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//input[contains(@id, 'FechaIni') or contains(@id, 'FechaDesde')]"))
            )
            input_fecha_ini.clear()
            input_fecha_ini.send_keys(fecha_inicio)

            fecha_fin = "31/12/2025"
            input_fecha_fin = driver.find_element(By.XPATH,
                                                  "//input[contains(@id, 'FechaFin') or contains(@id, 'FechaHasta')]")
            input_fecha_fin.clear()
            input_fecha_fin.send_keys(fecha_fin)
            time.sleep(1)

            # ==========================================
            # PASO 4: BUSCAR CONTRATO ESPECÍFICO
            # ==========================================
            print(f"Digitando ID de contrato: {contrato}")
            input_busqueda = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='search']"))
            )
            input_busqueda.clear()
            input_busqueda.send_keys(contrato)
            time.sleep(1)

            print("Haciendo clic en Consultar...")
            driver.find_element(By.XPATH, "//input[@type='submit' or @value='Consultar']").click()
            time.sleep(random.uniform(5.0, 7.0))

            print("Buscando el botón de acceso al contrato...")
            botones_resultados = driver.find_elements(By.XPATH, "//button[contains(@onclick, 'cto_consultar.aspx')]")

            if botones_resultados:
                print(f"¡Contrato {contrato} encontrado! Entrando...")
                driver.execute_script("arguments[0].click();", botones_resultados[0])
                time.sleep(4)

                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])

                id_secop_limpio = contrato.replace(".", "_").replace("-", "_")

                div_anexos = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, "ctl00_contentMain_divAnexos"))
                )
                filas = div_anexos.find_elements(By.XPATH, ".//table//tr[td]")
                print(f"Se detectaron {len(filas)} filas de documentos para este contrato.")

                for index, fila in enumerate(filas):
                    try:
                        descripcion_texto = fila.find_element(By.XPATH, "./td[2]").text
                        nombre_base = limpiar_nombre_archivo(descripcion_texto)
                        boton_descarga = fila.find_element(By.XPATH, "./td[3]//button[contains(@class, 'btn')]")

                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton_descarga)
                        time.sleep(0.5)

                        enlaces = fila.find_elements(By.XPATH, "./td[3]//ul//a | ./td[3]//a")
                        total_anexos = len(enlaces)

                        for sub_index in range(total_anexos):
                            consecutivo = sub_index + 1
                            nombre_final = f"ANEXO_{id_secop_limpio}_{nombre_base}_{consecutivo}"

                            # Asegurar menú abierto
                            driver.execute_script("arguments[0].click();", boton_descarga)
                            time.sleep(0.5)

                            enlaces_actualizados = fila.find_elements(By.XPATH, "./td[3]//ul//a | ./td[3]//a")
                            enlace_objetivo = enlaces_actualizados[sub_index]

                            # --------------------------------------------------
                            # TRUCO MÁGICO: CAPTURAR ESTADO ANTES DEL CLICK
                            # --------------------------------------------------
                            archivos_antes = set(os.listdir(carpeta_descargas))

                            print(f" -> [Fila {index + 1}] Descargando archivo {consecutivo} de {total_anexos}...")

                            from selenium.webdriver.common.action_chains import ActionChains
                            try:
                                acciones = ActionChains(driver)
                                acciones.move_to_element(enlace_objetivo).click().perform()
                            except Exception:
                                driver.execute_script("arguments[0].click();", enlace_objetivo)

                            # Esperar a que aparezca el archivo NUEVO correspondiente a este contrato
                            print("   Esperando descarga activa...")
                            archivo_detectado = None
                            intentos = 0

                            while intentos < 30:
                                time.sleep(1)
                                archivos_ahora = set(os.listdir(carpeta_descargas))
                                # Busquemos qué nombre apareció nuevo en la carpeta
                                nuevos = archivos_ahora - archivos_antes

                                # Filtrar que no sea un archivo temporal activo
                                nuevos_validos = [f for f in nuevos if
                                                  not f.endswith('.crdownload') and not f.endswith('.tmp')]

                                if nuevos_validos:
                                    archivo_detectado = nuevos_validos[0]
                                    break

                                intentos += 1

                            if archivo_detectado:
                                ruta_original = os.path.join(carpeta_descargas, archivo_detectado)
                                extension = os.path.splitext(archivo_detectado)[1]
                                ruta_nueva = os.path.join(carpeta_descargas, f"{nombre_final}{extension}")

                                # Evitar colisiones si ya existe
                                if os.path.exists(ruta_nueva):
                                    ruta_nueva = os.path.join(carpeta_descargas,
                                                              f"{nombre_final}_{int(time.time())}{extension}")

                                os.rename(ruta_original, ruta_nueva)
                                print(f"   Saved exitosamente: {os.path.basename(ruta_nueva)}")
                            else:
                                print(
                                    f"   ❌ Error: El servidor no respondió o la descarga tardó demasiado para el archivo {consecutivo}.")

                            time.sleep(1.5)

                    except Exception as e:
                        print(f"⚠️ Error procesando la fila {index + 1}: {e}")
                        continue

                print(f"¡Todos los anexos del contrato {contrato} procesados!")
            else:
                print(f"❌ No se encontró el contrato {contrato} en los resultados.")

    finally:
        driver.quit()
        print("Proceso finalizado.")

if __name__ == "__main__":
    # Desde aquí controlas tus búsquedas de forma limpia
    id_buscado = "1088-2025"
    depto = "Nariño"
    muni = "Pasto"
    ese = "E.S.E. PASTO SALUD"  # Cambia por "ESE SOACHA", etc., cuando quieras cambiar de contrato
    MI_USUARIO = "juridico3@pastosaludese.gov.co"  # Cambia por tu usuario real
    MI_CONTRASEÑA = "Ps@2024"




    url_base_datos_contratos = "1GpmxqMlDnSnMoDn7scsAw7e76ESW61oRnKL-bgwV6wM"
    url_hoja_contratos = f"https://docs.google.com/spreadsheets/d/{url_base_datos_contratos}/export?format=csv"
    df_contratos = pd.read_csv(url_hoja_contratos)
    df_contratos = df_contratos[df_contratos['resolucion'] == 873]  # Filtrar filas con número de proceso no nulo
    contratos = df_contratos['numero_de_proceso'].tolist()[160:560]




    driver , carpeta_salida = iniciar_sesion(
        usuario=MI_USUARIO,
        contrasenna=MI_CONTRASEÑA
    )
    descargar_soportes_sia(contratos, driver, carpeta_salida)