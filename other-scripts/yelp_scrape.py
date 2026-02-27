import json
import boto3
import requests
import time
from decimal import Decimal
from datetime import datetime

# configuration
YELP_API_KEY = 'CeCDFtH_FszwKGRxvx22TwiF1tb4vLyc3TmD2lCDxzxIjaCAQLKd3YnnOgCfkzI0hJCGZSxiw96Pqou3sT2FawIDIwWXGQJqOANsd3xxEXZa4FKzMUmR9ODHx12aaXYx'
YELP_ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
CUISINES = ['american', 'chinese', 'indian', 'italian', 'japanese', 'mexican', 'thai']
LOCATION = 'Manhattan, NY'
TABLE_NAME = 'yelp-restaurants'
REGION = 'us-east-1'

# create DynamoDB table
def create_table():
    dynamodb = boto3.client('dynamodb', region_name=REGION)
    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'BusinessID', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'BusinessID', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f'Creating table {TABLE_NAME}...')
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=TABLE_NAME)
        print('Table created successfully!')
    except dynamodb.exceptions.ResourceInUseException:
        print('Table already exists.')

# scrape Yelp
def scrape_yelp():
    headers = {'Authorization': f'Bearer {YELP_API_KEY}'}
    all_restaurants = {}

    for cuisine in CUISINES:
        print(f'\nScraping {cuisine} restaurants...')
        count = 0

        for offset in range(0, 250, 50):
            params = {
                'term': f'{cuisine} restaurants',
                'location': LOCATION,
                'limit': 50,
                'offset': offset,
                'categories': cuisine
            }

            response = requests.get(YELP_ENDPOINT, headers=headers, params=params)

            if response.status_code != 200:
                print(f'Error: {response.status_code} - {response.text}')
                break

            data = response.json()
            businesses = data.get('businesses', [])

            if not businesses:
                break

            for biz in businesses:
                biz_id = biz['id']
                if biz_id not in all_restaurants:
                    all_restaurants[biz_id] = {
                        'BusinessID': biz_id,
                        'Name': biz.get('name', ''),
                        'Address': ', '.join(biz['location'].get('display_address', [])),
                        'Coordinates': {
                            'Latitude': str(biz['coordinates'].get('latitude', '')),
                            'Longitude': str(biz['coordinates'].get('longitude', ''))
                        },
                        'NumberOfReviews': biz.get('review_count', 0),
                        'Rating': str(biz.get('rating', '')),
                        'ZipCode': biz['location'].get('zip_code', ''),
                        'Cuisine': cuisine,
                        'insertedAtTimestamp': datetime.now().isoformat()
                    }
                    count += 1

            time.sleep(0.5)

        print(f'Found {count} new {cuisine} restaurants')

    print(f'\nTotal unique restaurants: {len(all_restaurants)}')
    return all_restaurants

# load into DynamoDB
def load_to_dynamodb(restaurants):
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    count = 0
    for biz_id, restaurant in restaurants.items():
        item = json.loads(json.dumps(restaurant), parse_float=Decimal)
        table.put_item(Item=item)
        count += 1
        if count % 100 == 0:
            print(f'Loaded {count} restaurants...')

    print(f'Done! Loaded {count} restaurants into DynamoDB.')

# main
if __name__ == '__main__':
    print('Step 1: Creating DynamoDB table...')
    create_table()

    print('\nStep 2: Scraping Yelp...')
    restaurants = scrape_yelp()

    print('\nStep 3: Loading into DynamoDB...')
    load_to_dynamodb(restaurants)
