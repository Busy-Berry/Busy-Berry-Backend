import openai
import whisper
import datetime
import subprocess
import torch
import pyannote.audio
from pyannote.audio.pipelines.speaker_verification import PretrainedSpeakerEmbedding
import wave
import contextlib
from sklearn.cluster import AgglomerativeClustering
import numpy as np
import boto3
import json
import os
import re
from elasticsearch import Elasticsearch
import sys
import traceback

#config
bucket_name = 'busy-berry-meet-records'
access_key = 'AKIAZVXONNLLTFMZQJDT'
secret_key = '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'



def load_embedding_model():
    return PretrainedSpeakerEmbedding("speechbrain/spkrec-ecapa-voxceleb",device=torch.device("cuda:0"))

def load_audio(path):
    audio = pyannote.audio.Audio()
    with contextlib.closing(wave.open(path,'r')) as f:
        frames = f.getnframes()
        rate = f.getframerate()
        duration = frames / float(rate)
    return audio, duration

def segment_embedding(embedding_model, audio, path, segment, duration):
    start = segment["start"]
    end = min(segment["end"], duration)
    clip = pyannote.core.Segment(start, end)
    waveform, sample_rate = audio.crop(path, clip)
    try:
        return embedding_model(waveform[None])
    # many more statements like this
    except AssertionError:
        _, _, tb = sys.exc_info()
        traceback.print_tb(tb) # Fixed format

    
    
def preprocess_audio(path):
    if path[-3:] != 'wav':
        subprocess.call(['ffmpeg', '-i', path, 'audio.wav', '-y'])
        path = 'audio.wav'
    return path

def transcribe_audio(model, path):
    result = model.transcribe(path)
    return result["segments"]

def cluster_speakers(num_speakers, embeddings):
    clustering = AgglomerativeClustering(num_speakers).fit(embeddings)
    labels = clustering.labels_
    return labels

def format_time(secs):
    return datetime.timedelta(seconds=round(secs))

def write_transcript(segments, duration):
    transcript = ""
    for (i, segment) in enumerate(segments):
        if i == 0 or segments[i - 1]["speaker"] != segment["speaker"]:
            transcript += "\n" + segment["speaker"] + ' ' + str(format_time(segment["start"])) + '\n'
        transcript += segment["text"][1:] + ' '
    with open("transcript.txt", "w", encoding="utf-16") as f:
        f.write(transcript)
    return transcript

def generate_response(transcript, user_query):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
                {"role": "system", "content": f"Eres un asistente virtual, debes responder las preguntas que se te hagan de forma puntual y clara a partir de las transcripciones que se te entreguen, esta es la transcripcion de una sesion de un grupo de trabajo: \n{transcript}"},
                {"role": "user", "content": user_query},
            ]
    )
    return response.choices[0].message.content

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

def download_s3_folder(bucket_name, folder_name, local_path, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None):
    session_args = {
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
    }

    session = boto3.Session(**session_args)
    s3 = session.resource('s3')
    os.makedirs(folder_name)

    # Iterar sobre los objetos en la carpeta
    for obj in s3.Bucket(bucket_name).objects.filter(Prefix=folder_name):
        # Construir la ruta local de destino para cada objeto
        local_file_path = os.path.join( obj.key)
        # Si es un directorio, crear la carpeta local correspondiente
        if obj.key.endswith('/'):
            os.makedirs(local_file_path, exist_ok=True)
        else:
            # Descargar el archivo
            s3.meta.client.download_file(bucket_name, obj.key, local_file_path)

def change_format(commitments):
    # Dividir las líneas del texto
    if not "-" in commitments:
        return []
    lineas = commitments.split('\n')
    print(lineas)
    # Crear una lista para almacenar los compromisos convertidos
    compromisos_convertidos = []

    # Recorrer cada línea y extraer el responsable y el compromiso
    for linea in lineas:
        # Dividir la línea en responsable y compromiso utilizando el separador ' - '
        responsable, compromiso = linea.split(':')

        # Crear un diccionario para almacenar el compromiso convertido
        compromiso_convertido = {
            'assigned_to': responsable.strip(),  # Eliminar espacios en blanco alrededor del responsable
            'asignament': compromiso.strip()  # Eliminar espacios en blanco alrededor del compromiso
        }

        # Agregar el compromiso convertido a la lista
        compromisos_convertidos.append(compromiso_convertido)

    return compromisos_convertidos

def convertir_transcripcion(transcripcion):
    # Dividir las líneas del texto
    lineas = transcripcion.split('\n')
    lineas = [elemento for elemento in lineas if elemento != '']

    # Crear una lista para almacenar los fragmentos de la transcripción convertidos
    transcripcion_convertida = []

    # Recorrer cada línea y extraer el minuto, el hablante y el texto hablado
    for i in range(0, len(lineas), 2):
        linea_texto = lineas[i]
        linea_hablante = lineas[i + 1]
        if len(linea_hablante) < 3:
            continue
        # Extraer el hablante y el texto hablado
        hablante = linea_hablante.split(' ')[1]
        texto = linea_texto.strip()

        # Extraer el minuto del hablante utilizando expresiones regulares
        patron_minuto = re.search(r'\d+:\d+:\d+', linea_hablante)
        minuto = patron_minuto.group() if patron_minuto else ''

        # Crear un diccionario para almacenar el fragmento de la transcripción convertido
        fragmento_convertido = {
            'minute': minuto,
            'speaker': hablante,
            'speak': texto
        }

        # Agregar el fragmento de la transcripción convertido a la lista
        transcripcion_convertida.append(fragmento_convertido)

    return transcripcion_convertida

def indexar_documento_elasticsearch(documento, indice, id_documento=None):
    # Configurar la conexión con Elasticsearch
    host = 'my-deployment-7a1e92.es.us-east-2.aws.elastic-cloud.com:443' # URL de Elasticsearch
    username = 'elastic' # Nombre de usuario de Elasticsearch
    password = 'D5GcJhtuf79C0wdzUV673eUI' # Contraseña de Elasticsearch
    
    # Conexión a Elasticsearch
    es = Elasticsearch(
        [f"https://{username}:{password}@{host}"],
        request_timeout=30,
        verify_certs=False,
    )

    # Indexar el documento
    respuesta = es.index(index=indice, id=id_documento, body=documento)

    # Verificar el resultado
    if respuesta['result'] == 'created':
        print("Documento indexado correctamente.")
    elif respuesta['result'] == 'updated':
        print("Documento actualizado correctamente.")
    else:
        print("Error al indexar el documento:", respuesta)

def main():
    while True:
        folder_names = get_all_folders(bucket_name, access_key, secret_key)
        diferences = compare_and_update_array(folder_names)
        if len(diferences) > 0:
            for diff in diferences:
                download_s3_folder(bucket_name, diff, f"./{diff}", access_key, secret_key)
                openai.api_key = "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"

                num_speakers = 2

                language = 'any'

                model_size = 'small'

                model_name = model_size
                if language == 'English' and model_size != 'large':
                    model_name += '.en'

                path = f'C:/Users/Usuario/OneDrive/Escritorio/Busy Berry/Busy-Berry-Backend/Functions/Transcript/{diff}/audio.wav'
                path = preprocess_audio(path)

                embedding_model = load_embedding_model()
                audio, duration = load_audio(path)

                segments = transcribe_audio(whisper.load_model(model_size), path)

                embeddings = np.zeros(shape=(len(segments), 192))
                for i, segment in enumerate(segments):
                    embeddings[i] = segment_embedding(embedding_model, audio, path, segment, duration)

                embeddings = np.nan_to_num(embeddings)

                labels = cluster_speakers(num_speakers, embeddings)
                for i in range(len(segments)):
                    segments[i]["speaker"] = 'SPEAKER ' + str(labels[i] + 1)

                transcript = write_transcript(segments, duration)

                objetive = generate_response(transcript, "¿Cual fue el objetivo de la sesion?")
                description = generate_response(transcript, "Describe la sesion")
                commitments = change_format(generate_response(transcript, "¿Cuales fueron los compromisos posteriores a la sesion? debes darlo en formato: Responsable : Compromiso"))
                summary = generate_response(transcript, "Haz un resumen de la sesion")

                data = {
                        'organizer': 'Speaker 2',
                        'start_Time': '11:00 AM',
                        'end_Time': '1:00 PM',
                        'date': 'Thursday May 07, 2023',
                        'participants': ['Speaker 1', 'Speaker 2'],
                        'objective': objetive,
                        'description': description,
                        'summary':summary,
                        'commitments': commitments,
                        'transcription': convertir_transcripcion(transcript)
                    }
                indexar_documento_elasticsearch(data, "meet-records")
if __name__ == '__main__':
    main()