import os
import shutil

import pandas as pd
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
        r"D:\RES 1976-20260623T142856Z-3-001\RES 1976",
        r"D:\drive-download-20260623T143739Z-3-001",
    ]

DESTINO_ACTAS = "H:/1778/actas y certificados/actas_unificadas"
DESTINO_SUPERVISION = "H:/1778/actas y certificados/supervision_unificada"
DESTINO_CONTRATOS = r"D:\1976-DRIVE"


def unificar_archivos():
    print("Iniciando proceso de unificación de archivos APS...")

    # 1. Crear las carpetas de destino si aún no existen
    os.makedirs(DESTINO_ACTAS, exist_ok=True)
    os.makedirs(DESTINO_SUPERVISION, exist_ok=True)
    os.makedirs(DESTINO_CONTRATOS, exist_ok=True)

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

                        rename = f"CONTRATO_{archivo.split(" ")[2]}-2024.pdf"

                        ruta_archivo_destino = os.path.join(DESTINO_CONTRATOS, archivo)

                        # Copia el archivo original a la carpeta agrupada
                        shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)
                        contador_contratos_con_supervision += 1
                        print(f"  »  CONTRATOS unificada: {rename}")

                        # 🚀 EJECUTAR FILTRO GRIS (Opcional): Si también quieres las supervisiones en gris, descomenta la línea de abajo
                        convertir_pdf_a_gris(ruta_archivo_destino)

    print("\n" + "=" * 40)
    print("¡Proceso de unificación y conversión completado!")
    print(f"Total Actas unificadas y en gris: {contador_contratos_con_actas}")
    print(f"Total Supervisiones unificadas y en gris: {contador_contratos_con_supervision}")
    print("=" * 40)



def verficar_en_contratos():
    url_base_datos_contratos = "1GpmxqMlDnSnMoDn7scsAw7e76ESW61oRnKL-bgwV6wM"
    url_hoja_contratos = f"https://docs.google.com/spreadsheets/d/{url_base_datos_contratos}/export?format=csv"
    df_contratos = pd.read_csv(url_hoja_contratos)
    df_contratos = df_contratos[df_contratos['resolucion'] == 1976]  # Filtrar filas con número de proceso no nulo
    carpeta = r"D:\1976"
    contador = 0
    contratos = []
    for archivo in os.listdir(carpeta):
        nombre_archivo = archivo.split("_")[1].split(".")[0]
        if nombre_archivo in df_contratos["numero_de_proceso"].values:
            # print(f"✅ Contrato encontrado en base: {nombre_archivo}")
            contador += 1
            print(nombre_archivo)
            contratos.append(nombre_archivo)
            ruta_archivo_origen = os.path.join(carpeta, archivo)


            # # 3. Construimos la ruta de destino usando el NUEVO nombre (rename)
            ruta_archivo_destino = os.path.join(DESTINO_CONTRATOS,archivo)
            #
            # # 4. Copiamos desde la ruta origen completa a la ruta destino con el nuevo nombre
            shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)

            # contador_contratos_con_supervision += 1
            #print(f"  »  CONTRATOS unificada y renombrada: {nombre_archivo}")
        else:
            print(f"❌ Contrato NO encontrado en base: {nombre_archivo}")
            ds = 0

    df_contratos_faltante = df_contratos[~df_contratos["numero_de_proceso"].isin(contratos)]

    acatas = ["936-2024","875-2024","1040-2024","953-2024","874-2024","780-2024"]


    print(f"Total contratos encontrados en base: {contador} de {len(df_contratos)}")
def unificar_archivos_drive():
    for carpeta in CARPETAS:
        if not os.path.exists(carpeta):
            print(f"⚠️ La ruta no existe, saltando: {carpeta}")
            continue
        for archivo in os.listdir(carpeta):
            if archivo.startswith("CTO") and archivo.lower().endswith(".pdf"):
                ruta_archivo_origen = os.path.join(carpeta, archivo)
                nombre = archivo.split(" ")[2]

                rename = f"CONTRATO_{nombre}-2025.pdf"

                # 3. Construimos la ruta de destino usando el NUEVO nombre (rename)
                ruta_archivo_destino = os.path.join(DESTINO_CONTRATOS, rename)

                # 4. Copiamos desde la ruta origen completa a la ruta destino con el nuevo nombre
                shutil.copy2(ruta_archivo_origen, ruta_archivo_destino)

                # contador_contratos_con_supervision += 1
                print(f"  »  CONTRATOS unificada y renombrada: {rename}")

                # 🚀 EJECUTAR FILTRO GRIS (Opcional): Si también quieres las supervisiones en gris, descomenta la línea de abajo
                convertir_pdf_a_gris(ruta_archivo_destino)

def renombrar_orden_nombre_archivos(carpeta_base):
    contratos_escaneados = []
    for archivo in os.listdir(carpeta_base):
        contrato = archivo.split("_")
        nombre_contrato = f"{contrato[1]}-{contrato[2]}"
        contratos_escaneados.append(nombre_contrato)

    # borra duplicados
    contratos_escaneados = list(set(contratos_escaneados))
    print(f"Contratos escaneados: {contratos_escaneados}")

        # if archivo.__contains__("ESTUDIOS") and archivo.lower().endswith(".pdf"):
        #     n = archivo.split("_")
        # 
        #     nombre_nuevo = f"{n[0]}_{n[1]}_{n[2]}_AB_{n[3]}_{n[-1]}.pdf"
        #     print(f"{nombre_nuevo}")
        #     ruta_archivo_origen = os.path.join(carpeta_base, archivo)
        # 
        # 
        #     # 4. Reenombramos el archivo en la misma carpeta
        #     ruta_archivo_destino = os.path.join(carpeta_base, nombre_nuevo)
        #     os.rename(ruta_archivo_origen, ruta_archivo_destino)
        #     print(f"  »  CONTRATOS renombrada: {nombre_nuevo}")


def condiconal_es_acta_valida(archivo):
    df_actas_validas = pd.read_csv(r"F:\APS AUTOMATIZACIONES\aps\automatizacion_looker_aps\contratacion_cruzada\contratos_para_enviar.csv")
    nombre = archivo.split(" ")[3]
    nombre = nombre.replace("-1", "")
    nombre = nombre.replace(".pdf", "")
    nombre = nombre + "-2025"

    if nombre in df_actas_validas["numero_contrato"].values:
        return True
    else:
        return False

if __name__ == "__main__":
    # Ahora que el flujo está conectado de forma dinámica, puedes ejecutar la función principal directamente
    #unificar_archivos_drive()
    #verficar_en_contratos()
    #unificar_archivos()
    # renombrar_orden_nombre_archivos(r'H:\APS RES 873 JULIO\RESOLUCION 873 2025 - 2026\3. Contratos o actos administrativos formalizados para la ejecución de los recursos\contrato_Res_900_1')
    crear_pdf_maestro_agreesivo(carpeta_base=r'H:\APS RES 873 JULIO\RESOLUCION 873 2025 - 2026\5. Actas de ejecución de los recursos parciales y finales suscritas por el supervisor o interventor. (Acta de pago mensual por talento humano, transporte)\5.1\ACTAS',
                                codicion_especifica=condiconal_es_acta_valida,
                                nombre_maestro="SER124SREC20260630NI000900091143ID2087325712D05.pdf",
                                nombre_a_unificar="PAGO", contratos_opcion=False)


# PRIMERO DESCARGAR SIA DESPUES DIVIDIR TAMAAÑO
# SEGUNDO PASO: UNIFICAR ARCHIVOS Y CONVERTIR A GRIS SEGUN NOMMBRE
# INFORMES DE SUPERVICION UNIFICAR SEGUN LOS DESCARGADOS
#TERCERO ORDENES DE PAGO UNIFICAR SEGUN LOS DESCARGADOS