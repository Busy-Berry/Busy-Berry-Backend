import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection

def lambda_handler(event, context):
    # Configuración de Elasticsearch
    host = 'xx' # URL de Elasticsearch
    username = 'yy' # Nombre de usuario de Elasticsearch
    password = 'zz' # Contraseña de Elasticsearch
    
    # Conexión a Elasticsearch
    es = Elasticsearch(
        hosts=[{'host': host}],
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    # Consulta en Elasticsearch por un documento específico por su ID
    doc_id = event['doc_id'] # Obtener el ID del documento de la entrada del evento
    res = es.get(index='meet_records', id=doc_id)
    
    # Procesamiento de resultados
    record = res['_source']
    
    # Devolución de respuesta
    response = {
        "statusCode": 200,
        "body": json.dumps(record)
    }
    return response
