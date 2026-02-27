import json
import boto3
import random
import requests
from requests.auth import HTTPBasicAuth

# configuration
OPENSEARCH_ENDPOINT = 'https://search-restaurants-2udyf2kuhnxbyon3s5dexjvycu.us-east-1.es.amazonaws.com'
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASS = 'Admin789!'
INDEX_NAME = 'restaurants'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/826402425288/DiningRequestsQueue'
TABLE_NAME = 'yelp-restaurants'
REGION = 'us-east-1'
SENDER_EMAIL = 'tk2766@nyu.edu'

# create service connections
sqs = boto3.client('sqs', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
ses = boto3.client('ses', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    # check SQS queue for a dining request
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )

    # no messages
    if 'Messages' not in response:
        print('No messages in queue.')
        return

    # grab message and its delete token
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])

    # get user's dining preferences from message
    cuisine = body['Cuisine']
    email = body['Email']
    num_people = body['NumberOfPeople']
    dining_date = body['DiningDate']
    dining_time = body['DiningTime']
    location = body['Location']

    print(f'Processing request: {cuisine} food for {num_people} in {location}')

    # search OpenSearch for restaurants by cuisine
    search_url = f'{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_search'
    query = {
        "size": 50,
        "query": {
            "match": {
                "Cuisine": cuisine.lower()
            }
        }
    }

    # send search request to OpenSearch with credentials
    auth = HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASS)
    headers = {'Content-Type': 'application/json'}
    os_response = requests.get(search_url, auth=auth, headers=headers, json=query)
    results = os_response.json()

    # get list of matching restaurants
    hits = results['hits']['hits']
    if not hits:
        print(f'No restaurants found for {cuisine}')
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        return

    # pick 3 random restaurants from result
    selected = random.sample(hits, min(3, len(hits)))

    # look up full details in DynamoDB
    restaurants = []
    for hit in selected:
        restaurant_id = hit['_source']['RestaurantID']
        db_response = table.get_item(Key={'BusinessID': restaurant_id})
        if 'Item' in db_response:
            restaurants.append(db_response['Item'])

    # build and send email of restaurant suggestions
    restaurant_list = ''
    for i, r in enumerate(restaurants, 1):
        restaurant_list += f"{i}. {r['Name']} - {r['Address']} - Rating: {r['Rating']}/5 - Reviews: {r['NumberOfReviews']}\n"

    # compose email
    email_body = (
        f"Hello! Here are my {cuisine.title()} restaurant suggestions in {location.title()} "
        f"for {num_people} people on {dining_date} at {dining_time}:\n\n"
        f"{restaurant_list}\n"
        f"Enjoy your meal!"
    )

    # send email
    ses.send_email(
        Source=SENDER_EMAIL,
        Destination={'ToAddresses': [email]},
        Message={
            'Subject': {'Data': f'Your {cuisine.title()} Restaurant Suggestions'},
            'Body': {'Text': {'Data': email_body}}
        }
    )

    print(f'Email sent to {email}')

    # delete message from SQS so it doesn't get processed again
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
    print('Message deleted from queue.')
