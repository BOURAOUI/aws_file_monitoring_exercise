import json
import pandas as pd
import awswrangler as wr

def lambda_handler(event, context):
    # TODO implement
    print ("Event received")

    #vérifier si j'ai la clef Records dans le message de SQS, si je ne l'ai pas je sors de la fonction
    if "Records" not in event:
        return {"statusCode": 200, "body": json.dumps("pas de records")}

    print ("un Record existe")

    for record in event['Records']:
        if "body" not in record:
            return {"statusCode": 200, "body": json.dumps("pas de body dans record")}
        else:
            body_dict = json.loads(record['body'])
            print (body_dict)
            bucket_name = body_dict['detail']['bucket']['name']
            file_size = body_dict['detail']['object']['size']
            event_time = body_dict['time']
            file_name = body_dict['detail']['object']['key']
            print(f"Bucket name : {bucket_name}, File size : {file_size}, Event time : {event_time}, File name : {file_name}")
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
            print(df)
            # Sauvegarde au format Parquet
            path_s3 = "s3://raw-bucket-s3-2-dev/output.parquet"
            wr.s3.to_parquet(df=df, path=path_s3, index=False)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



