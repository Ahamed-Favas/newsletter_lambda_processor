import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('newsletter-jobStatus')

def lambda_handler(event, context):

    job_id = event.get('queryStringParameters', {}).get('jobId')
    if not job_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'jobId is required'})
        }
    
    try:
        response = table.get_item(Key={'jobId': job_id})
        item = response.get('Item')
        if not item:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Job not found'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error retrieving job status'})
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'jobId': job_id,
            'status': item.get('status'),
            'result': item.get('result')
        })
    }

