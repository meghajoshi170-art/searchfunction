import boto3
import json
import base64
import urllib.parse
import logging
import requests
from requests_aws4auth import AWS4Auth

# ----------------------
# Configuration
# ----------------------
REGION = 'us-east-1'
SERVICE = 'es'
HOST = 'https://vpc-opensearchdomain-tachsh6xhkkxruazssvq3j7qxy.aos.us-east-1.on.aws'
INDEX = 'opensearch'
HEADERS = {"Content-Type": "application/json"}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Auth for OpenSearch
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    SERVICE,
    session_token=credentials.token
)

# ----------------------
# Query OpenSearch
# ----------------------
def get_from_search(query):
    url = f"{HOST}/{INDEX}/_search"  # Correct _search endpoint
    print(url)
    try:
        response = requests.post(url, auth=awsauth, headers=HEADERS, data=json.dumps(query))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"OpenSearch query failed: {str(e)}")
        raise

# ----------------------
# Lambda handler
# ----------------------
def lambda_handler(event, context):
    try:
        logger.info(f"Event: {json.dumps(event)}")
        term = None

        # GET request with query param ?q=
        if event.get("queryStringParameters") and "q" in event["queryStringParameters"]:
            term = event["queryStringParameters"]["q"]

        # POST request body
        elif event.get("body"):
            body = event["body"]
            if event.get("isBase64Encoded"):
                body = base64.b64decode(body).decode("utf-8")
            # Try JSON body first
            try:
                parsed = json.loads(body)
                term = parsed.get("searchTerm")
            except Exception:
                # If JSON fails, fallback to form-data
                form_dict = urllib.parse.parse_qs(body)
                if "searchTerm" in form_dict:
                    term = form_dict["searchTerm"][0]

        if not term:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"status": False, "message": "Missing search term"})
            }

        # Build OpenSearch query
        query = {
            "size": 25,
            "query": {
                "multi_match": {
                    "query": term,
                    "fields": ["Title", "Author", "Date", "Body"]
                }
            },
            "_source": ["Title", "Author", "Date", "Summary"]
        }

        # Call OpenSearch
        response_json = get_from_search(query)
        hits = response_json.get("hits", {}).get("hits", [])
        flattened =  [
            {
                "fields": h["_source"],
                "_score": h["_score"]                
            }
            for h in hits
            ]

        # Return search results
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(flattened)
        }

    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": False, "message": str(e)})
        }