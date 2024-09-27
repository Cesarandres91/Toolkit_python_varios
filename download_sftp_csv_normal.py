import paramiko
from io import BytesIO
import pandas as pd
from datetime import datetime

# Configuración SFTP
hostname = 'tu_servidor_sftp'
username = 'tu_usuario'
password = 'tu_contraseña'
port = 22  # Puerto estándar para SFTP

# Generar el nombre del archivo
today = datetime.now().strftime('%d%m%Y')
filename = f'data_{today}.csv'
remote_path = f'folder1/fol/data/{filename}'

# Conexión SFTP
transport = paramiko.Transport((hostname, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)

# Descargar el archivo
try:
    with BytesIO() as buf:
        sftp.getfo(remote_path, buf)
        buf.seek(0)
        df = pd.read_csv(buf)
    
    print(f'Archivo {filename} descargado exitosamente.')
    print(f'Dimensiones del DataFrame: {df.shape}')
    print('\nPrimeras 5 filas del DataFrame:')
    print(df.head())

except FileNotFoundError:
    print(f'El archivo {filename} no se encontró en el servidor SFTP.')
except Exception as e:
    print(f'Ocurrió un error: {str(e)}')

finally:
    # Cerrar conexión SFTP
    sftp.close()
    transport.close()

# Aquí puedes continuar con el procesamiento del DataFrame si la descarga fue exitosa
# Por ejemplo:
# if 'df' in locals():
#     df_processed = process_data(df)
#     df_processed.to_csv('processed_file.csv', index=False)
