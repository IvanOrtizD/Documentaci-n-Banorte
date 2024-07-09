from google.cloud import bigquery
from datetime import datetime
import os

# Establece la variable de entorno para el archivo de credenciales de la cuenta de servicio
# Esto es necesario para autenticar la aplicación con Google Cloud.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "C:/Users/JoaquínFonsecaVázque/OneDrive/PROYECTOS/BANORTE/banorte_ksm.json"
)

# Crea un cliente de BigQuery
client = bigquery.Client()

# Configura las variables necesarias
project_id = "poc-dataai-kd"
dataset_id = "test_replicate"
location = "us"
kms_key_name = f"projects/{project_id}/locations/{location}/keyRings/ksm_banorte/cryptoKeys/bq-dataset_banorte"
logging_table_id = "poc-dataai-kd.banorte_ksm.tablas_no_encriptadas"

# Referencia al dataset
dataset_ref = client.dataset(dataset_id, project=project_id)

try:
    # Lista todas las tablas en el dataset
    tables = client.list_tables(dataset_ref)

    # Itera sobre las tablas y ejecuta el comando COPY TABLE para cambiar el cifrado
    for table in tables:
        table_id = f"{project_id}.{dataset_id}.{table.table_id}"

        print(f"Ejecutando copia para la tabla {table_id}:")
        
        # Configuración del trabajo de copia
        job_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Sobrescribe la tabla de destino
            destination_encryption_configuration=bigquery.EncryptionConfiguration(
                kms_key_name=kms_key_name
            ),
        )
        
        try:
            # Ejecuta el trabajo de copia de la tabla sobre sí misma para cambiar el cifrado
            copy_job = client.copy_table(
                table_id,
                table_id,  # Copia sobre sí misma
                location=location,
                job_config=job_config,
            )
            copy_job.result()  # Espera a que el trabajo se complete
            print(f"Tabla {table_id} copiada y encriptada con éxito.")

        except Exception as e:
            print(f"Error al copiar o encriptar la tabla {table_id}: {str(e)}")

            # Inserta un registro en la tabla de logging si ocurre un error
            rows_to_insert = [
                {
                    "project": project_id,
                    "dataset": dataset_id,
                    "table": table.table_id,
                    "update": datetime.now().isoformat(),
                    "comments": str(e),
                }
            ]
            errors = client.insert_rows_json(logging_table_id, rows_to_insert)
            if errors == []:
                print(f"Registro insertado en {logging_table_id}")
            else:
                print(f"Error al insertar el registro en {logging_table_id}: {errors}")
except Exception as e:
    print(f"Error al listar las tablas en el dataset: {str(e)}")

print("Proceso completado.")
