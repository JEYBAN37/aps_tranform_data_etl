import subprocess


def ejecutar_comando_mega(comando):
    """Ejecuta un comando MEGAcmd y devuelve la salida."""
    try:
        # Ejecuta el comando en la consola
        proceso = subprocess.Popen(['mega-cmd', comando], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proceso.communicate()  # Obtiene la salida y los errores

        if stderr:
            print(f"Error ejecutando el comando: {stderr}")
        return stdout
    except FileNotFoundError:
        return "Error: El comando 'mega-cmd' no se encuentra. Asegúrate de que MEGAcmd esté instalado y en el PATH."


# Ejemplo de uso: listar los archivos en la nube
print(ejecutar_comando_mega('ls'))