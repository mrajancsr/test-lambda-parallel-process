# - the purpose of this file is to send messages to a sqs queue to invoke a lambda function on trigger
# the triggered lambda function computes a covariance matrix that i end up storing in a distributed queue system
# then i obtain the value from the queue for the following tickers

import logging
import math
import time
from typing import Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.client("sqs")
queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/test_queue"
result_queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/result_queue"


def receive_message(queue, queue_url):
    response = queue.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
    )
    return response


def send_messages_in_batches(queue, messages, queue_url):
    """
    Send a batch of messages in a single request to an SQS queue.
    This request may return overall success even when some messages were not sent.
    The caller must inspect the Successful and Failed lists in the response and
    resend any failed messages.

    :param queue: The queue to receive the messages.
    :param messages: The messages to send to the queue. These are simplified to
                     contain only the message body and attributes.
    :return: The response from SQS that contains the list of successful and failed
             messages.
    """
    try:
        entries = [
            {
                "Id": str(ind),
                "MessageBody": msg["body"],
            }
            for ind, msg in enumerate(messages)
        ]
        response = queue.send_message_batch(
            Entries=entries,
            QueueUrl=queue_url,
        )
        if "Successful" in response:
            for msg_meta in response["Successful"]:
                logger.info(
                    "Message sent: %s: %s",
                    msg_meta["MessageId"],
                    messages[int(msg_meta["Id"])]["body"],
                )
        if "Failed" in response:
            for msg_meta in response["Failed"]:
                logger.warning(
                    "Failed to send: %s: %s",
                    msg_meta["MessageId"],
                    messages[int(msg_meta["Id"])]["body"],
                )
    except ClientError as error:
        logger.exception("Send messages failed to queue: %s", queue)
        raise error
    else:
        return response


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


def batch_records(records: List[Any], batch_size: int) -> List[List[Any]]:
    num_batches: int = math.ceil(len(records) / batch_size)

    result = []
    for i in range(num_batches):
        result.append(records[i * batch_size : (i + 1) * batch_size])
    yield from result


if __name__ == "__main__":

    # sending a single message
    # MessageBody = "Testing one to three, raju is a handsome boy and so is neptune"
    # send_message(sqs, message_body=MessageBody, queue_url=queue_url)

    messages = [
        {"body": ticker}
        for ticker in [
            "AAPl",
            "BTC",
            "ETH",
            "ADA",
            "LUNA",
            "GOOG",
            "MATIC",
            "POLKADOT",
            "SOL",
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "DODGECOIN",
            "SHIBAINU",
            "MORGAN STANLEY",
            "JP MORGAN",
            "BEST BUY",
            "TARGET",
            "TESLA",
            "AMAZON",
            "TWITTER",
            "BENZ",
            "BMW",
            "HONDA",
            "TOYOTA",
        ]
    ]
    for batch in batch_records(messages, 10):
        response = send_messages_in_batches(sqs, batch, queue_url=queue_url)
        # receive message from the queue

    message_size = len(messages)
    count = 0
    message = None
    result = []
    while count < message_size:
        try:
            message = receive_message(sqs, queue_url=result_queue_url)
            if message is None:
                continue
        except Exception as e:
            print(e)
        else:
            print("message received")
            result.append(message)
            count += len(message)

    print(result)
