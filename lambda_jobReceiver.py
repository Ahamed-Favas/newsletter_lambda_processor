import json
import uuid
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('newsletter-jobStatus')
lambda_client = boto3.client('lambda')


def clear_db():
    """Deletes all items in the DynamoDB table before inserting a new record."""
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'jobId': item['jobId']})

def lambda_handler(event, context):

    job_id = str(uuid.uuid4())
    
    try:
        
        clear_db()

        table.put_item(Item={
            'jobId': job_id,
            'status': 'processing',
            'result': None
        })
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error creating job record'})
        }

    try:
        lambda_client.invoke(
            FunctionName='newsletter_processor',  # name of the processing Lambda
            InvocationType='Event',
            Payload=json.dumps({'jobId': job_id, 'input': event.get('body')})
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error invoking processing function'})
        }
    
    # Immediately return the job id to the client.
    return {
        'statusCode': 202,
        'body': json.dumps({'jobId': job_id, 'message': 'Job started'})
    }

