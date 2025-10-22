import base64
import gzip
import json
import pandas as pd

def lambda_handler(event, context):
    # TODO implement
    print ("Event received")

    # Décoder le contenu gzip/base64 envoyé par CloudWatch Logs
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)

    print ("Donnée décodé : ", json.dumps(payload, indent=2))

    #vérifier si j'ai la clef Records dans le message de SQS, si je ne l'ai pas je sors de la fonction
    if "Records" not in payload:
        return {"statusCode": 200, "body": json.dumps("pas de records")}

    for record in payload['Records']:
        if "body" not in record:
            return {"statusCode": 200, "body": json.dumps("pas de body dans record")}
        else:
            body_dict = json.loads(record['body'])

    #s3_event = evenement S3 unique
    record_list = body_dict['Records']
    s3_event = record_list[0]

    bucket_name = s3_event['s3']['bucket']['name']
    file_size = s3_event['s3']['object']['size']
    event_time = s3_event['time']
    file_name = s3_event['s3']['object']['key']

    #dataframe
    df = pd.DataFrame (
            [
                {
                    "file_name": file_name,
                    "bucket_name": bucket_name,
                    "file_size": file_size,
                    "event_time": event_time,
                }
            ]
        )

    # Sauvegarde au format Parquet
    df.to_parquet("output.parquet", index=False)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



