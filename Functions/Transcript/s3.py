import boto3
import json


def get_all_folders(bucket_name, access_key, secret_key):
    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')

    folders = []
    if 'CommonPrefixes' in response:
        prefixes = response['CommonPrefixes']
        folders = [prefix['Prefix'].rstrip('/') for prefix in prefixes]

    return folders

def compare_and_update_array(new_array):
    try:
        # Cargar el contenido del archivo
        file_path = "data.json"
        with open(file_path, 'r') as file:
            existing_array = json.load(file)
    except FileNotFoundError:
        existing_array = []

    # Comparar los arrays
    difference = list(set(new_array) - set(existing_array))

    if difference:
        # Actualizar el archivo con el nuevo array
        with open(file_path, 'w') as file:
            json.dump(new_array, file)

    return difference



# Reemplazar 'nombre-del-bucket' con el nombre real de tu bucket de S3
bucket_name = 'busy-berry-meet-records'
access_key = 'AKIAZVXONNLLTFMZQJDT'
secret_key = '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'

folder_names = get_all_folders(bucket_name, access_key, secret_key)
print(compare_and_update_array(folder_names))