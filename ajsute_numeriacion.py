def main():
    ruta = "./reportes/2025-12-10/REPORTES/consolidado/APS124CCFP20251208NI000900091143.txt"

    # Leer líneas
    with open(ruta, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    nuevas_lineas = []
    contador = 1

    for linea in lineas:
        partes = linea.strip().split("|")

        # Solo procesamos si hay suficientes columnas
        if len(partes) >= 2:
            partes[1] = str(contador)  # MODIFICAR columna 2
            contador += 1

        nuevas_lineas.append("|".join(partes) + "\n")

    # Guardar en el mismo archivo
    with open(ruta, "w", encoding="utf-8") as f:
        f.writelines(nuevas_lineas)

    print("Columna 2 actualizada correctamente.")


if __name__ == "__main__":
    main()