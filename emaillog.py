from datetime import datetime, timedelta
from pathlib import Path
import sys
import os
import os.path
from xxlimited import Null
import boto3
from botocore.exceptions import ClientError
import time
import json


AWS_CONFIG = {
    "access_key": "AKIAQABZ2UU66TGN2JPR",
    "secret_key": "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",
}

AWS_S3 = {"region": "ap-southeast-1",
          "bucket": "redback-event-dev", "bucket_log": "sitesjet-log"}

LIBRE_OFFICE = {"home": ".", "path": "libreoffice"}

AWS_DYNAMODB = {"region": "ap-southeast-1"}
AWS_DYNAMODB_TABLE_NAME = "Kapara"

AWS_DYNAMODB_CLIENT = boto3.client(
    "dynamodb",
    region_name=AWS_DYNAMODB["region"],
    aws_access_key_id=AWS_CONFIG["access_key"],
    aws_secret_access_key=AWS_CONFIG["secret_key"],
)

S3_FILTERED = "Contents[?Size > `10`]"

S3_RESOURCE = boto3.resource(
    's3', aws_access_key_id=AWS_CONFIG['access_key'], aws_secret_access_key=AWS_CONFIG['secret_key'])
S3_CLIENT = boto3.client(
    's3', aws_access_key_id=AWS_CONFIG['access_key'], aws_secret_access_key=AWS_CONFIG['secret_key'])

# BODY = '{"emailID":"12345", "SentDate":"2023/04/04", "email":"bima.ferdiansyah@sitesjet.com"}'


# ========
# function
# ========


def json_manipulation(data, event_type):
    event_email_id = data["mail"]["tags"]["rbEmailID"][0]
    sentdate = data["mail"]["tags"]["rbSentDate"][0]
    email = data["mail"]["commonHeaders"]["to"][0]

    if event_email_id != None:
        # mysql_save(event_email_id, email, sentdate, event_type)
        email_log_input = create_input_log(event_email_id, email, sentdate, event_type)
        res = execute_put_item(email_log_input)
        if res:
            response = {
                "statusCode": 200,
                "body": "success"
            }

            return response



def create_input_log(event_email_id, email, sentdate, event_type):
    items = {
        "EventID": {"S": "EmailLog"},
        "SK": {"S": "EV-{}".format(event_email_id)},
    }

    items["Email"] = {"S": "{}".format(email)}

    items["SentDate"] = {"S": "{}".format(sentdate)}

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
    
    event_data = json.loads(event)
    event_type = event_data['eventType'].lower()


    json_manipulation(event_data, event_type)
