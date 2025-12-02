# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from datetime import datetime
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_info(message, **kwargs):
    """Log structured JSON messages for better analysis"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "message": message,
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    log_info("Processing request", table_name=table, request_id=context.request_id)
    
    if event["body"]:
        item = json.loads(event["body"])
        log_info("Received payload", item=item, request_id=context.request_id)
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        log_info("Data inserted successfully", item_id=id, request_id=context.request_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        log_info("Received request without payload", request_id=context.request_id)
        item_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": item_id},
            },
        )
        message = "Successfully inserted data!"
        log_info("Default data inserted", item_id=item_id, request_id=context.request_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
