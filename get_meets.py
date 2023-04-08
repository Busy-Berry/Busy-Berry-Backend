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
    
    # Consulta en Elasticsearch con filtros de rango y ordenación
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"organizer": event['organizer']}},
                    {"match": {"participants": event['participants']}}
                ],
                "filter": {
                    "range": {
                        "date": {
                            "gte": event['start_date'],
                            "lte": event['end_date']
                        }
                    }
                }
            }
        },
        "sort": [
            {event['sort_by']: {"order": event['sort_order']}}
        ]
    }
    res = es.search(index='meet_records', body=query)
    
    # Procesamiento de resultados
    records = []
    for hit in res['hits']['hits']:
        record = hit['_source']
        records.append(record)
    
    # Devolución de respuesta
    response = {
        "statusCode": 200,
        "body": json.dumps(records)
    }
    return response
