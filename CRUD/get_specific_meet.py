import json
from elasticsearch import Elasticsearch

def lambda_handler(event, context):
    try:
        # Configuración de Elasticsearch
        host = 'xx' # URL de Elasticsearch
        username = 'yy' # Nombre de usuario de Elasticsearch
        password = 'zz' # Contraseña de Elasticsearch
        
        # Conexión a Elasticsearch
        es = Elasticsearch(
    	    [f"{username}:{password}@{host}"],
    	    scheme="https",
    	    request_timeout=30,
    	    verify_certs=False,
        )
        
        # Consulta en Elasticsearch por un documento específico por su ID
        body = json.loads(event["body"])
        doc_id = body['doc_id'] # Obtener el ID del documento de la entrada del evento
        res = es.get(index='meet_records', id=doc_id)
        
        # Procesamiento de resultados
        record = res['_source']
        
        # Devolución de respuesta
        response = {
            "statusCode": 200,
            "body": json.dumps(record)
        }
        return response
    except Exception as e:
        response = {
            "statusCode": 500,
            "body": json.dumps(event)
        }
        return response

