from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import paramiko
from io import BytesIO
import pandas as pd

def download_from_sftp(**kwargs):
    # Configuración SFTP
    hostname = 'tu_servidor_sftp'
    username = 'tu_usuario'
    password = 'tu_contraseña'
    port = 22  # Puerto estándar para SFTP

    # Generar el nombre del archivo
    today = kwargs['execution_date'].strftime('%d%m%Y')
    filename = f'data_{today}.csv'
    remote_path = f'folder1/fol/data/{filename}'

    # Conexión SFTP
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Descargar el archivo
    with BytesIO() as buf:
        sftp.getfo(remote_path, buf)
        buf.seek(0)
        df = pd.read_csv(buf)

    # Cerrar conexión SFTP
    sftp.close()
    transport.close()

    # Aquí puedes continuar con el procesamiento del DataFrame
    # Por ejemplo:
    # df_processed = process_data(df)
    # df_processed.to_csv('/path/to/processed_file.csv', index=False)

    return f'Archivo {filename} descargado y procesado'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sftp_etl',
    default_args=default_args,
    description='ETL con descarga de archivo SFTP',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='download_and_process',
    python_callable=download_from_sftp,
    provide_context=True,
    dag=dag,
)
