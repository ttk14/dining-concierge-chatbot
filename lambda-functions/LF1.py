import json
import datetime
import boto3

# accepted values for validation
ALLOWED_CITIES = ['new york', 'new york city', 'nyc', 'manhattan']
ALLOWED_CUISINES = ['american', 'chinese', 'indian', 'italian', 'japanese', 'mexican', 'thai']

def validate_slots(slots):
    # validate Location is in ALLOWED_CITIES
    if slots.get('Location') and slots['Location'].get('value'):
        location = slots['Location']['value']['interpretedValue'].lower()
        if location not in ALLOWED_CITIES:
            return {
                'isValid': False,
                'violatedSlot': 'Location',
                'message': 'Sorry, we only support Manhattan at the moment. Please enter a valid location.'
            }

    # validate Cuisine is in ALLOWED_CUISINES
    if slots.get('Cuisine') and slots['Cuisine'].get('value'):
        cuisine = slots['Cuisine']['value']['interpretedValue'].lower()
        if cuisine not in ALLOWED_CUISINES:
            return {
                'isValid': False,
                'violatedSlot': 'Cuisine',
                'message': f"We don't support that cuisine yet. Please choose from: {', '.join(ALLOWED_CUISINES).title()}."
            }

    # validate DiningDate is not in the past
    if slots.get('DiningDate') and slots['DiningDate'].get('value'):
        dining_date = slots['DiningDate']['value']['interpretedValue']
        try:
            date_obj = datetime.datetime.strptime(dining_date, '%Y-%m-%d').date()
            if date_obj < datetime.date.today():
                return {
                    'isValid': False,
                    'violatedSlot': 'DiningDate',
                    'message': 'You cannot book for a past date. Please enter a future date.'
                }
        except ValueError:
            return {
                'isValid': False,
                'violatedSlot': 'DiningDate',
                'message': 'Please enter a valid date.'
            }   

    # validate NumberOfPeople is in range 1-20
    if slots.get('NumberOfPeople') and slots['NumberOfPeople'].get('value'):
        try:
            num = int(slots['NumberOfPeople']['value']['interpretedValue'])
            if num < 1 or num > 20:
                return {
                    'isValid': False,
                    'violatedSlot': 'NumberOfPeople',
                    'message': 'Please enter a valid number of people (1-20).'
                }
        except ValueError:
            return {
                'isValid': False,
                'violatedSlot': 'NumberOfPeople',
                'message': 'Please enter a valid number.'
            }

    return {'isValid': True}

# main function that handles all Lex events
def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent']['slots']
    source = event['invocationSource']  # 'DialogCodeHook' or 'FulfillmentCodeHook'

    if intent_name == 'DiningSuggestionsIntent':
        # dialog code hook: validate slots as user provides them
        if source == 'DialogCodeHook':
            validation_result = validate_slots(slots)

            # if a slot is invalid, ask user to correct the invalid slot with error message
            if not validation_result['isValid']:
                return {
                    "sessionState": {
                        "dialogAction": {
                            "slotToElicit": validation_result['violatedSlot'],
                            "type": "ElicitSlot"
                        },
                        "intent": {"name": intent_name, "slots": slots}
                    },
                    "messages": [{"contentType": "PlainText", "content": validation_result['message']}]
                }

            # all slots validated, let Lex ask for the next one
            return {
                "sessionState": {
                    "dialogAction": {"type": "Delegate"},
                    "intent": {"name": intent_name, "slots": slots}
                }
            }

        # fulfilment code hook: all slots filled, send request to SQS
        if source == 'FulfillmentCodeHook':
            sqs = boto3.client('sqs', region_name='us-east-1')
            
            # send message to SQS queue for LF2 processing
            sqs.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/826402425288/DiningRequestsQueue',
                MessageBody=json.dumps({
                    'Location': slots['Location']['value']['interpretedValue'],
                    'Cuisine': slots['Cuisine']['value']['interpretedValue'],
                    'DiningDate': slots['DiningDate']['value']['interpretedValue'],
                    'DiningTime': slots['DiningTime']['value']['interpretedValue'],
                    'NumberOfPeople': slots['NumberOfPeople']['value']['interpretedValue'],
                    'Email': slots['Email']['value']['interpretedValue']
                })
            )

            # send confirmation message
            return {
                "sessionState": {
                    "dialogAction": {"type": "Close"},
                    "intent": {"name": intent_name, "slots": slots, "state": "Fulfilled"}
                },
                "messages": [{"contentType": "PlainText", "content": "You're all set. My suggestions are on the way! Have a good day."}]
            }

    # greeting response
    elif intent_name == 'GreetingIntent':
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "slots": slots, "state": "Fulfilled"}
            },
            "messages": [{"contentType": "PlainText", "content": "Hi there, how can I help?"}]
        }

    # thank you response
    elif intent_name == 'ThankYouIntent':
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "slots": slots, "state": "Fulfilled"}
            },
            "messages": [{"contentType": "PlainText", "content": "You're welcome! Have a great day."}]
        }
