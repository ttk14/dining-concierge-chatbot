# Dining Concierge Chatbot

An AI-powered chatbot that provides restaurant recommendations based on user preferences, built on AWS.

## Architecture

- **Frontend:** Static website hosted on Amazon S3
- **API Gateway:** REST API that connects the frontend to the backend
- **Lambda LF0:** Receives user messages and forwards them to Amazon Lex
- **Amazon Lex:** NLU bot that collects dining preferences (location, cuisine, date, time, party size, email)
- **Lambda LF1:** Validates user inputs during the Lex conversation and pushes completed requests to SQS
- **Amazon SQS:** Message queue that holds dining requests for asynchronous processing
- **Lambda LF2:** Queue worker triggered by CloudWatch every minute, searches OpenSearch for matching restaurants, retrieves details from DynamoDB, and sends recommendations via SES
- **Amazon OpenSearch:** Search index for quickly finding restaurants by cuisine
- **Amazon DynamoDB:** NoSQL database storing 1,377 restaurant records scraped from Yelp
- **Amazon SES:** Sends email with restaurant recommendations to the user

## Repository Structure
```
├── frontend/           # S3 hosted chatbot UI
├── lambda-functions/   # LF0, LF1, LF2 Lambda code
└── other-scripts/      # Yelp scraping and OpenSearch loading scripts
```
