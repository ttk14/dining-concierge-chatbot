import json
import boto3
import requests
from requests.auth import HTTPBasicAuth

# configuration
OPENSEARCH_ENDPOINT = 'https://search-restaurants-2udyf2kuhnxbyon3s5dexjvycu.us-east-1.es.amazonaws.com'
INDEX_NAME = 'restaurants'
MASTER_USER = 'admin'
MASTER_PASSWORD = 'Admin789!'
TABLE_NAME = 'yelp-restaurants'
REGION = 'us-east-1'

auth = HTTPBasicAuth(MASTER_USER, MASTER_PASSWORD)
headers = {'Content-Type': 'application/json'}

# create the index
def create_index():
    url = f'{OPENSEARCH_ENDPOINT}/{INDEX_NAME}'
    
    requests.delete(url, auth=auth) # delete index if it already exists
    
    # create index with mapping
    mapping = {
        "mappings": {
            "properties": {
                "RestaurantID": {"type": "keyword"},
                "Cuisine": {"type": "keyword"}
            }
        }
    }
    
    response = requests.put(url, auth=auth, headers=headers, json=mapping)
    print(f'Create index: {response.status_code} - {response.text}')

# load data from DynamoDB into OpenSearch
def load_data():
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)
    
    # scan all items from DynamoDB
    print('Scanning DynamoDB...')
    items = []
    response = table.scan()
    items.extend(response['Items'])
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    print(f'Found {len(items)} restaurants in DynamoDB')
    
    # load each item into OpenSearch
    count = 0
    for item in items:
        doc = {
            'RestaurantID': item['BusinessID'],
            'Cuisine': item['Cuisine']
        }
        
        url = f'{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_doc/{item["BusinessID"]}'
        response = requests.put(url, auth=auth, headers=headers, json=doc)
        
        count += 1
        if count % 100 == 0:
            print(f'  Loaded {count} restaurants...')
    
    print(f'Done! Loaded {count} restaurants into OpenSearch.')

# verify
def verify():
    url = f'{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_count'
    response = requests.get(url, auth=auth)
    print(f'Total documents in OpenSearch: {response.json().get("count", 0)}')

if __name__ == '__main__':
    print('Step 1: Creating index...')
    create_index()
    
    print('\nStep 2: Loading data...')
    load_data()
    
    print('\nStep 3: Verifying...')
    verify()
