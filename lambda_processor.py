import json
import boto3
import requests
from bs4 import BeautifulSoup
import logging
import time
import functools

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('newsletter-jobStatus')

bedrock = boto3.client(
    service_name='bedrock-runtime',
    region_name="us-east-1"
)

MODEL_ID = "us.meta.llama3-1-8b-instruct-v1:0"

def backoff(delay=1, retries=3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_retry = 0
            current_delay = delay
            while current_retry < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    current_retry += 1
                    if current_retry >= retries:
                        raise e
                    logger.warning(f"failed to execute function '{func.__name__}'. Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= 2
        return wrapper
    return decorator

@backoff(delay=1, retries=3)
def get_news_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response

@backoff(delay=1, retries=3)
def get_ai_summary(item_content, item_link):
    prompt = f"Create a very short summary (2-3 sentences) of the following news content, only the summary is required, dont say here is the summary etc... :\n\n{item_content}"
    formatted_prompt = f"""
    <|begin_of_text|><|start_header_id|>user<|end_header_id|>
    {prompt}
    <|eot_id|>
    <|start_header_id|>assistant<|end_header_id|>
    """

    bedrock_body = {
        "prompt": formatted_prompt,
        "temperature": 0.5,  # Lower temperature for more focused responses
        "top_p": 0.9,       # More conservative sampling
        "stop": ["User:", "Model:"]
    }

    try:
        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(bedrock_body)
        )

        response_body = json.loads(response.get('body').read().decode('utf-8'))
        item_summary = response_body.get('generation', '').strip()
        logger.info("Summary generated for %s", item_link)
        return item_summary

    except Exception as e:
        logger.error("Error generating summary for %s: %s", item_link, str(e))
        return ""


def lambda_handler(event, context):
    try:
        job_id = event.get('jobId')
        logger.info(f"Received jobId: {job_id}")

        input_data = json.loads(event.get('input'))
        news_items = input_data.get('news', [])

        summaries = []

        for item in news_items:
            item_link = item.get('Link')
            item_contentClass = item.get('contentClass')
            item_category = item.get('category')
            item_index = item.get('index')

            html_response = get_news_content(item_link)
            if html_response.status_code != 200:
                logger.error(f"Failed to fetch {item_link}. Status code: {html_response.status_code}")
                continue
            
            item_html = html_response.text
            item_content = ""

            soup = BeautifulSoup(item_html, 'html.parser')
            elements = soup.find_all(class_=item_contentClass)

            if not elements:
                logger.warning(f"No elements found with class {item_contentClass} in {item_link}")
                continue

            for element in elements:
                item_content += element.get_text()

            logger.info(f"fetched content for {item_link} : {item_content[:50]}")
            
            item_summary = get_ai_summary(item_content, item_link)

            logger.info(f"fetched summary for {item_link} : {item_summary[:50]}")

            summaries.append({
                'category': item_category,
                'indexStr': item_index,
                'description': item_summary
            })

        result = {'summaries': summaries}

        table.update_item(
            Key={'jobId': job_id},
            UpdateExpression="SET #s = :status, #r = :result",
            ExpressionAttributeNames={'#s': 'status', '#r': 'result'},
            ExpressionAttributeValues={
                ':status': 'completed',
                ':result': json.dumps(result)
            }
        )
        return

    except Exception as e:
        logger.error("Error processing request: %s", str(e))
        # Update with failure status
        table.update_item(
            Key={'jobId': job_id},
            UpdateExpression="SET #s = :status, #e = :error",
            ExpressionAttributeNames={'#s': 'status', '#e': 'error'},
            ExpressionAttributeValues={
                ':status': 'failed',
                ':error': str(e)
            }
        )
        return