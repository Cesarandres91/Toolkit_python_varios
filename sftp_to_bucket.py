import paramiko
from io import BytesIO
import pandas as pd
from datetime import datetime
from google.cloud import storage

# Configuración SFTP
hostname = 'tu_servidor_sftp'
username = 'tu_usuario'
password = 'tu_contraseña'
port = 22  # Puerto estándar para SFTP

# Configuración GCP
bucket_name = 'nombre_de_tu_bucket'
gcp_project_id = 'tu_proyecto_id'

# Generar el nombre del archivo
today = datetime.now().strftime('%d%m%Y')
filename = f'data_{today}.csv'
remote_path = f'folder1/fol/data/{filename}'

# Función para cargar archivo a GCP
def upload_to_gcp(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client(project=gcp_project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(source_file_name.getvalue(), content_type='text/csv')
    print(f"Archivo {destination_blob_name} cargado a {bucket_name}.")

# Conexión SFTP
transport = paramiko.Transport((hostname, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)

# Descargar el archivo y cargar a GCP
try:
    with BytesIO() as buf:
        sftp.getfo(remote_path, buf)
        buf.seek(0)
        df = pd.read_csv(buf)
    
        print(f'Archivo {filename} descargado exitosamente.')
        print(f'Dimensiones del DataFrame: {df.shape}')
        print('\nPrimeras 5 filas del DataFrame:')
        print(df.head())

        # Preparar el archivo para cargarlo a GCP
        buf.seek(0)
        upload_to_gcp(bucket_name, buf, filename)

except FileNotFoundError:
    print(f'El archivo {filename} no se encontró en el servidor SFTP.')
except Exception as e:
    print(f'Ocurrió un error: {str(e)}')

finally:
    # Cerrar conexión SFTP
    sftp.close()
    transport.close()

# Aquí puedes continuar con el procesamiento del DataFrame si es necesario
# Por ejemplo:
# if 'df' in locals():
#     df_processed = process_data(df)
#     df_processed.to_csv('processed_file.csv', index=False)
