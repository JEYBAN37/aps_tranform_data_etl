import glob
import os
import shutil
import uuid
import getpass
import pandas as pd
import win32com.client
from PIL import Image
import customtkinter as ctk
from tkinter import filedialog, messagebox
import io
import requests

# --- CONFIGURACIÓN DE APARIENCIA ---
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class AppDigitalizacion(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.url_base_datos = "1WnG6EBin4IxEwhcg0aOtC0efh9k_fg7dWVXrV86ZUfY"
        self.url_base_datos_contratos = "1GpmxqMlDnSnMoDn7scsAw7e76ESW61oRnKL-bgwV6wM"
        self.ejecutable_script = "AKfycbybbDArDM06_8wegb7D7iVDGhrLC5rw79kl-0PJ-YeA6KyIW2BlzaLcJrcinurm1SRx"
        self.title("PASTO SALUD E.S.E. - Sistema de Digitalización APS")
        self.geometry("700x550")
        self.carpeta_base = ""
        self.df_pagos = None
        self.df_contratos = None
        self.ruta_contrato_actual = ""
        self.acta_actual = ""
        self.one_drive = ""

        # UI: Layout
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # Sidebar
        self.sidebar_frame = ctk.CTkFrame(self, width=140, corner_radius=15)
        self.sidebar_frame.grid(row=0, column=0, rowspan=4, sticky="nsew", padx=20, pady=20)
        self.logo_label = ctk.CTkLabel(self.sidebar_frame, text="APS DIGITAL",
                                       font=ctk.CTkFont(size=20, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))

        self.btn_cambiar_base = ctk.CTkButton(self.sidebar_frame, text="Configurar Carpeta Raíz",
                                              command=self.configurar_base)
        self.btn_cambiar_base.grid(row=1, column=0, padx=20, pady=10)

        self.btn_cambiar_base = ctk.CTkButton(self.sidebar_frame, text="Configurar Carpeta OneDrive",
                                              command=self.configurar_onedrive)
        self.btn_cambiar_base.grid(row=2, column=0, padx=40, pady=10)

        # Main Frame
        self.main_frame = ctk.CTkFrame(self, corner_radius=15)
        self.main_frame.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")

        self.lbl_titulo = ctk.CTkLabel(self.main_frame, text="Búsqueda por Número de Acta",
                                       font=ctk.CTkFont(size=18, weight="bold"))
        self.lbl_titulo.pack(pady=10)

        # Entrada única: Número de Acta
        self.entry_acta = ctk.CTkEntry(self.main_frame, placeholder_text="Ingrese Número de Acta (Orden)",
                                       width=300,
                                       height=40)
        self.entry_acta.pack(pady=10)

        self.btn_preparar = ctk.CTkButton(self.main_frame, text="Buscar y Preparar Destino", fg_color="#2980b9",
                                          command=self.preparar_por_acta)
        self.btn_preparar.pack(pady=10)

        # Info del Contrato Detectado
        self.lbl_info_contrato = ctk.CTkLabel(self.main_frame, text="Esperando número de acta...",
                                              text_color="gray")
        self.lbl_info_contrato.pack(pady=10)

        # Panel de Acciones (Botones de Escaneo)
        self.acciones_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        # El panel se mostrará solo cuando se encuentre el acta

        self.btn_escanear_acta = ctk.CTkButton(self.acciones_frame, text="📑 ESCANEAR ESTA ACTA (MESA)",
                                               command=lambda: self.iniciar_escaneo("ACTA"))
        self.btn_escanear_acta.pack(pady=5, fill="x")

        self.btn_escanear_sup = ctk.CTkButton(self.acciones_frame, text="🛡️ ESCANEAR SUPERVISIÓN (MESA)",
                                              command=lambda: self.iniciar_escaneo("SUPERVISION"))
        self.btn_escanear_sup.pack(pady=5, fill="x")

        self.btn_celular_acta = ctk.CTkButton(self.acciones_frame, text="📸 IMPORTAR ESTA ACTA (MÓVIL)",
                                              fg_color="#d35400", hover_color="#e67e22",
                                              command=lambda: self.escanear_desde_celular("ACTA"))
        self.btn_celular_acta.pack(pady=5, fill="x")

        self.btn_celular_sup = ctk.CTkButton(self.acciones_frame, text="📸 IMPORTAR SUPERVISIÓN (MÓVIL)",
                                             fg_color="#d35400", hover_color="#e67e22",
                                             command=lambda: self.escanear_desde_celular("SUPERVISION"))
        self.btn_celular_sup.pack(pady=5, fill="x")

        self.status_label = ctk.CTkLabel(self.main_frame, text="Offline", text_color="gray")
        self.status_label.pack(side="bottom", pady=10)

        self.after(1000, self.cargar_base_desde_nube)
        self.after(100, self.configurar_base)

    def preparar_por_acta(self):
        num_acta = self.entry_acta.get().strip()
        if not num_acta:
            messagebox.showwarning("Atención", "Ingrese un número de acta primero.")
            return

        if self.df_pagos is None:
            messagebox.showerror("Error", "Cargue la carpeta base primero.")
            return

        # 1. Buscar acta en df_pagos para obtener el contrato
        fila_pago = self.df_pagos[self.df_pagos["orden"] == num_acta]

        if fila_pago.empty:
            messagebox.showerror("No encontrado", f"El acta {num_acta} no existe en la base de pagos.")
            self.acciones_frame.pack_forget()
            return

        contrato_id = str(fila_pago["numero_contrato"].values[0])
        self.acta_actual = num_acta

        # 2. Buscar contrato en df_contratos para obtener la resolución
        fila_contrato = self.df_contratos[self.df_contratos["numero_contrato"] == contrato_id]

        if fila_contrato.empty:
            messagebox.showerror("Error",
                                 f"El contrato {contrato_id} asociado a esta acta no está en la base de contratos.")
            return

        # 1. Limpiamos el ID que viene del Excel (quitamos espacios y posibles decimales)
        resolucion_id_crudo = fila_contrato['resolucion'].values[0]

        # 2. Tu tabla de equivalencias
        RESLUCIONES = [
            ("ID2177823635", "1778"),
            ("ID2197624614", "1976"),
            ("ID2087325712", "873"),
            ("ID2139724657", "1397"),
        ]

        # 3. Búsqueda con validación
        resolucion = next((res[1] for res in RESLUCIONES if res[0] == resolucion_id_crudo), None)

        # 4. Verificación de seguridad
        if resolucion is None:
            messagebox.showerror("Error de Carpeta",
                                 f"El ID de recurso '{resolucion_id_crudo}' no está mapeado a ninguna resolución conocida.")
            return

        # 3. Construir ruta y crear carpetas
        self.ruta_contrato_actual = os.path.join(self.carpeta_base, resolucion, contrato_id)
        os.makedirs(self.ruta_contrato_actual, exist_ok=True)

        # 4. Mostrar acciones
        self.lbl_info_contrato.configure(
            text=f"✅ Acta: {num_acta} | Contrato: {contrato_id}\n📂 Ubicación: {resolucion}/{contrato_id}",
            text_color="#3498db"
        )
        self.acciones_frame.pack(pady=10, fill="x", padx=50)
        self.status_label.configure(text="Listo para digitalizar", text_color="#2ecc71")

    def iniciar_escaneo(self, tipo_documento):
        if not self.ruta_contrato_actual: return

        if tipo_documento == "ACTA":
            nombre_final = f"ACTA_PAGO_{self.acta_actual}"
        else:
            id_u = str(uuid.uuid4())[:4]
            nombre_final = f"SUPERVISION_ACTA_{self.acta_actual}_{id_u}"

        self.status_label.configure(text=f"⏳ Escaneando...", text_color="orange")
        self.update()

        try:
            wia = win32com.client.Dispatch("WIA.CommonDialog")
            img_file = wia.ShowAcquireImage()

            if img_file:
                temp_jpg = os.path.join(self.ruta_contrato_actual, "temp.jpg")
                img_file.SaveFile(temp_jpg)

                with Image.open(temp_jpg) as img:
                    bw = img.convert("L").point(lambda p: 255 if p > 128 else 0, mode="1")
                    pdf_path = os.path.join(self.ruta_contrato_actual, f"{nombre_final}.pdf")
                    bw.convert("RGB").save(pdf_path, "PDF", resolution=300.0)

                os.remove(temp_jpg)
                messagebox.showinfo("Éxito", f"Guardado: {nombre_final}.pdf")

                if tipo_documento == "ACTA":
                    self.marcar_como_escaneado_en_nube(self.acta_actual)

            self.status_label.configure(text="Completado", text_color="#2ecc71")
        except Exception as e:
            messagebox.showerror("Error", f"Falla escáner: {e}")

    def escanear_desde_celular(self, tipo_documento):


        try:
            archivos = glob.glob(os.path.join(self.one_drive, "*.pdf"))
            if not archivos:
                messagebox.showwarning("OneDrive", "No hay PDFs nuevos en la carpeta 'escaneos'.")
                return

            pdf_reciente = max(archivos, key=os.path.getmtime)

            if tipo_documento == "ACTA":
                nombre_final = f"ACTA_PAGO_{self.acta_actual}.pdf"
            else:
                id_u = str(uuid.uuid4())[:4]
                nombre_final = f"SUPERVISION_ACTA_{self.acta_actual}_{id_u}.pdf"

            shutil.move(pdf_reciente, os.path.join(self.ruta_contrato_actual, nombre_final))
            messagebox.showinfo("Éxito", f"Importado: {nombre_final}")

            if tipo_documento == "ACTA":
                self.marcar_como_escaneado_en_nube(self.acta_actual)

        except Exception as e:
            messagebox.showerror("Error", f"Error al importar: {e}")

    def marcar_como_escaneado_en_nube(self, num_acta):
        # URL de tu Apps Script (la que obtienes al darle "Implementar")
        url_script = f"https://script.google.com/macros/s/{self.ejecutable_script}/exec"

        try:
            params = {'acta': num_acta}
            # allow_redirects es obligatorio para Google Apps Script
            respuesta = requests.get(url_script, params=params, allow_redirects=True)

            if "Éxito" in respuesta.text:
                messagebox.showinfo("Marcado en la nube",
                                    f"El acta {num_acta} ha sido marcada como escaneada en la nube.")
            else:
                print(f"Advertencia: El script no encontró el acta {num_acta}")
        except Exception as e:
            print(f"Error de conexión con la nube: {e}")

    def cargar_sheet(self, url_hoja):
        try:
            respuesta = requests.get(url_hoja)
            respuesta.raise_for_status()  # Verifica que la descarga fue exitosa
            return pd.read_csv(io.StringIO(respuesta.content.decode('utf-8')))
        except Exception as e:
            messagebox.showerror("Error de Conexión", f"No se pudo acceder a la hoja en la nube:\n{e}")
            return None

    def cargar_base_desde_nube(self):
        # Enlace que copiaste de "Publicar en la web"
        url_hoja_pagos = f"https://docs.google.com/spreadsheets/d/{self.url_base_datos}/export?format=csv"
        url_hoja_contratos = f"https://docs.google.com/spreadsheets/d/{self.url_base_datos_contratos}/export?format=csv"

        self.status_label.configure(text="⏳ Sincronizando con la nube...", text_color="orange")
        self.update()

        # Descargar los datos

        self.df_pagos = self.cargar_sheet(url_hoja_pagos)
        # Limpieza de datos (igual que antes)
        self.df_pagos["orden"] = self.df_pagos["orden"].astype(str).str.strip().str.split('.').str[0]
        self.df_pagos["numero_contrato"] = self.df_pagos["numero_contrato"].astype(str).str.strip()

        self.df_contratos = self.cargar_sheet(url_hoja_contratos)
        self.df_contratos["numero_contrato"] = self.df_contratos["numero_contrato"].astype(str).str.strip()

        self.status_label.configure(text="✅ Base de datos sincronizada", text_color="#2ecc71")


    def configurar_base(self):
        self.carpeta_base = filedialog.askdirectory(title="Seleccione carpeta 'contratacion_cruzada'")
        if self.carpeta_base:
            try:
                self.status_label.configure(text=f"Bases cargadas correctamente", text_color="#2ecc71")
            except Exception as e:
                messagebox.showerror("Error", f"Falla al cargar archivos Excel:\n{e}")

    def configurar_onedrive(self):
        self.one_drive = filedialog.askdirectory(title="Seleccione carpeta 'escaneos' en OneDrive")
        if self.carpeta_base:
            try:
                self.status_label.configure(text=f"deposito cargado correctamente", text_color="#2ecc71")
            except Exception as e:
                messagebox.showerror("Error", f"Falla al cargar archivos Excel:\n{e}")




if __name__ == "__main__":
    app = AppDigitalizacion()
    app.mainloop()
