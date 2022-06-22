import json
import logging
from multiprocessing import Pipe, Process
from typing import List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.client("sqs")
lambda_client = boto3.client("lambda")


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


def compute_covar(ticker: str, conn):
    print(f"processing for ticker {ticker}")
    event = {"model_name": "compute_covariance", "ticker": ticker}
    res = lambda_client.invoke(
        FunctionName="generate_standard_normal", Payload=json.dumps(event)
    )
    response = send_message(
        queue=sqs,
        message_body=res["Payload"].read().decode("utf-8"),
        queue_url=result_queue_url,
    )
    conn.send([response])
    conn.close()


def compute_covar_for_all_tickers(tickers: List[str]):
    processes = []
    parent_connections = []
    for ticker in tickers:
        parent_conn, child_conn = Pipe()
        parent_connections.append(parent_conn)

        process = Process(
            target=compute_covar,
            args=(
                ticker,
                child_conn,
            ),
        )
        processes.append(process)

    # start all processes
    for process in processes:
        process.start()

    # make sure that all processes have finished
    for process in processes:
        process.join()

    return [parent_conn.recv()[0] for parent_conn in parent_connections]


def lambda_handler(event, context):
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
        print("testing multiprocessing")
        tickers = [record.get("body") for record in event["Records"]]
        print(compute_covar_for_all_tickers(tickers))
        print(f"finished processing for tickers {tickers}")


if __name__ == "__main__":
    messages = [
        {"body": ticker}
        for ticker in [
            "AAPl",
            "BTC",
            "ETH",
            "ADA",
        ]
    ]
    event = {"Records": messages}
    lambda_handler(event, None)
