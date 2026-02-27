import json
import boto3

# connect to Lex V2 runtime
lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

# main handler function
def lambda_handler(event, context):
    try:
        # parse user's message from request body 
        body = json.loads(event.get('body', '{}'))
        user_message = body.get('messages', [{}])[0].get('unstructured', {}).get('text', '')

        # check if user_message is empty or None
        if not user_message:
            return build_response('Sorry, I didn\'t understand that.')

        # send user's message to Lex V2 and get response
        lex_response = lex_client.recognize_text(
            botId='IGAAGFC8AV',
            botAliasId='TSTALIASID',
            localeId='en_US',
            sessionId='default-user',
            text=user_message
        )

        # extract bot's response
        bot_messages = lex_response.get('messages', [])
        if bot_messages:
            bot_reply = bot_messages[0]['content']
        else:
            bot_reply = 'Sorry, I didn\'t understand that.'

        return build_response(bot_reply)

    except Exception as e:
        print(f"Error: {str(e)}")
        return build_response('Something went wrong. Please try again.')


# format response for API Gateway and the frontend
def build_response(message_text):
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps({
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': message_text
                }
            }]
        })
    }
