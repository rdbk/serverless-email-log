from datetime import datetime, timedelta
from pathlib import Path
import sys
import os
import re
import os.path
from xxlimited import Null
import boto3
from botocore.exceptions import ClientError
import time
import json


CONFIG_AWS_REGION = os.environ['CONFIG_AWS_REGION']
CONFIG_AWS_KEY_ID = os.environ['CONFIG_AWS_KEY_ID']
CONFIG_AWS_SECRET_KEY = os.environ['CONFIG_AWS_SECRET_KEY']

AWS_DYNAMODB_TABLE_NAME = "Kapara"

AWS_DYNAMODB_CLIENT = boto3.client(
    "dynamodb",
    region_name=CONFIG_AWS_REGION,
    aws_access_key_id=CONFIG_AWS_KEY_ID,
    aws_secret_access_key=CONFIG_AWS_SECRET_KEY,
)

S3_FILTERED = "Contents[?Size > `10`]"

S3_RESOURCE = boto3.resource(
    's3', aws_access_key_id=CONFIG_AWS_KEY_ID, aws_secret_access_key=CONFIG_AWS_SECRET_KEY)
S3_CLIENT = boto3.client(
    's3', aws_access_key_id=CONFIG_AWS_KEY_ID, aws_secret_access_key=CONFIG_AWS_SECRET_KEY)

# BODY = '{"emailID":"12345", "SentDate":"2023/04/04", "email":"bima.ferdiansyah@sitesjet.com"}'


# ========
# function
# ========


def json_manipulation(data, event_type):
    event_email_id = data["mail"]["tags"]["rbEmailID"][0]
    event_id = data["mail"]["tags"]["rbEventID"][0]
    sentdate = data["mail"]["tags"]["rbSentDate"][0]
    post_id = data["mail"]["tags"]["rbPostID"][0]

    email = data["mail"]["commonHeaders"]["to"][0]
    if isValidEmail(email) != True:
        return get_response('Invalid email parameter', 102)

    if event_email_id != None:
        # mysql_save(event_email_id, email, sentdate, event_type)
        email_log_input = create_input_log(event_id, event_email_id, email, sentdate, event_type, post_id)
        if True == is_dynamodb_none():
            return get_response("Internal error - database connection issue", 201)

        res = execute_put_item(email_log_input)
        if res:
            response = {
                "statusCode": 200,
                "body": "success"
            }

            return response

def get_response(msg, error=1, data=None):
    body = {
        "error": error,
        "msg": msg,
        "login_url": data
    }

    response = {
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            },
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def isValidEmail(email):
    if len(email) > 7:
        if re.match("[^@]+@[^@]+\.[^@]+", email) != None:
            return True
    return False

def is_dynamodb_none():
    try:
        AWS_DYNAMODB_CLIENT.describe_table(
            TableName=AWS_DYNAMODB_TABLE_NAME)
        return False
    except AWS_DYNAMODB_CLIENT.exceptions.ResourceNotFoundException:
        return True

def create_input_log(event_id, event_email_id, email, sentdate, event_type, post_id):
    items = {
        "EventID": {"S": "EV-{}".format(event_id)},
        "SK": {"S": "EmailLog-{}".format(event_email_id)},
    }

    items["Email"] = {"S": "{}".format(email)}

    items["SentDate"] = {"S": "{}".format(sentdate)}

    items["PostID"] = {"S": "{}".format(post_id)}


    return {"TableName": AWS_DYNAMODB_TABLE_NAME, "Item": items}

def execute_put_item(input):
    try:
        response = AWS_DYNAMODB_CLIENT.put_item(**input)
        print("Successfully put item.")
        return response
    except ClientError as error:
        return error.response["Error"]["Message"]
    except BaseException:
        return "Error BaseException"


def emaillog(event, context):

    if 'body' not in event:
        return get_response("Parameter not found", 100)
    
    event_data = json.loads(event)

    event_type = event_data['eventType'].lower()


    json_manipulation(event_data, event_type)
