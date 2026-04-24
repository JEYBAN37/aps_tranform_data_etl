import os

import fitz
import pandas as pd
from automatizacion_looker_aps.contratacion_cruzada.convertir_contratacion import optimizar_pdf_agresivo


def crear_pdf_maestro_agreesivo(carpeta_base: str, nombre_maestro: str = "VACUNACION.pdf",
                                nombre_a_unificar: str = "CTO") -> str | None:
    ruta_maestro = os.path.join(carpeta_base, nombre_maestro)
    ruta_sin_opt = os.path.join(carpeta_base, "MAESTRO_SIN_OPTIMIZAR.pdf")

    # ── Paso 1: unir todos sin optimizar ────────────────────────
    archivos_unificados = []

    ruta_sub = os.path.join(carpeta_base, 'VACUNACIÓN')

    for archivo in os.listdir(ruta_sub):
        if archivo.startswith(nombre_a_unificar) and archivo.endswith(".pdf"):
            archivos_unificados.append(os.path.join(ruta_sub, archivo))
            # break

    if not archivos_unificados:
        print(f"⚠️  No se encontraron archivos {nombre_a_unificar}")
        return None


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
    print(f"\n🗜  Optimizando a 96 DPI / JPEG 45%...")
    stats = optimizar_pdf_agresivo(ruta_sin_opt, ruta_maestro)

    print(f"\n✅ PDF maestro → {nombre_maestro}")
    print(f"   📊 {stats['original_mb']}MB → {stats['final_mb']}MB (ahorro {stats['ahorro_pct']}%)")
    print(f"   {'✅ Bajo 500MB' if stats['final_mb'] <= 500 else '⚠️  Aún sobre 500MB — bajar JPEG_MAESTRO a 35'}")

    # Limpiar temporal
    if os.path.exists(ruta_sin_opt):
        os.remove(ruta_sin_opt)

    return ruta_maestro


if __name__ == "__main__":
    crear_pdf_maestro_agreesivo(carpeta_base='C:/Users/acer/Downloads')