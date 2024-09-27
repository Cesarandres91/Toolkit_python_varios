import zipfile
import os
import shutil

def encrypt_zip(input_file, output_file, password):
    # Crear una copia del archivo original
    shutil.copy2(input_file, output_file)

    # Abrir el archivo copiado en modo de anexar y establecer la contraseña
    with zipfile.ZipFile(output_file, 'a') as zf:
        zf.setpassword(password.encode())

    print(f"El archivo {output_file} ha sido encriptado exitosamente.")

def process_folder(input_folder, output_folder, password):
    # Asegurarse de que la carpeta de salida exista
    os.makedirs(output_folder, exist_ok=True)

    # Procesar cada archivo en la carpeta de entrada
    for filename in os.listdir(input_folder):
        if filename.endswith('.zip'):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)
            encrypt_zip(input_path, output_path, password)

# Uso del script
input_folder = 'FOLDER1'
output_folder = 'FOLDER2'
password = 'tu_contraseña_aquí'

process_folder(input_folder, output_folder, password)
print(f"Todos los archivos ZIP en {input_folder} han sido procesados y guardados en {output_folder}.")
