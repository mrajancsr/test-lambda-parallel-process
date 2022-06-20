import json
import logging
import math
import time
from typing import Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.client("sqs")
lambda_client = boto3.client("lambda")
queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/test_queue"
result_queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/result_queue"


def send_message(queue, message_body, message_attributes=None, queue_url=""):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes,
            QueueUrl=queue_url,
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response


def handler(event, context):
    if "action" in event:
        if event["action"] == "compute_covar":
            event = {"model_name": "compute_covariance", "ticker": "test123"}
            response = lambda_client.invoke(
                FunctionName="generate_standard_normal",
                Payload=json.dumps(event),
            )
            send_message(
                sqs,
                message_body=response["Payload"].read().decode("utf-8"),
                queue_url=result_queue_url,
            )
    elif "Records" in event:
        records = event["Records"]
        for record in records:
            ticker = record.get("body")
            event = {"model_name": "compute_covariance", "ticker": ticker}
            response = lambda_client.invoke(
                FunctionName="generate_standard_normal", Payload=json.dumps(event)
            )
            send_message(
                sqs,
                response["Payload"].read().decode("utf-8"),
                queue_url=result_queue_url,
            )


def receive_message(queue, queue_url):
    response = queue.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=22,
        WaitTimeSeconds=10
    )
    return response


if __name__ == "__main__":
    event = {"action": "compute_covar"}
    handler(event, None)
