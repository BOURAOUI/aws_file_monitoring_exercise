import base64
import gzip
import json

def lambda_handler(event, context):
    # TODO implement
    print ("Event received")

    # Décoder le contenu gzip/base64 envoyé par CloudWatch Logs
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)

    print ("Donnée décodé : ", json.dumps(payload, indent=2))

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



