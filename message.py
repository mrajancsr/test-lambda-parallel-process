# The file computes the covariance matrix by sending messages as tickers to a
# distributed queue in AWS which triggers a lambda function concurrently
# each triggered lambda function is running a parallel processing for
# batch of tickers

# -- To run the file, go the terminal and type python3 message.py

import asyncio
import json
import logging
import math
import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import uvloop
from aiobotocore.session import AioBaseClient, get_session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

MESSAGE_QUEUE = "test_queue"
RESULT_QUEUE = "result_queue"

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def poll_queue_for_batch(
    client: AioBaseClient,
    queue_name: str,
    batch_size: int,
) -> asyncio.Queue:
    """Polls a queue in AWS given by queue_name

    Parameters
    ----------
    client : AioBaseClient
        connects to sqs service
    queue_name : str
        registered queue in AWS
    batch_size : int
        size of the records in batch

    Returns
    -------
    asyncio.Queue
        Queue containing messages in AWS
    """
    print(f"receiving message from {queue_name}")
    queue = asyncio.Queue()
    delete_messages = []
    try:
        response = await client.get_queue_url(QueueName=queue_name)
        queue_url = response.get("QueueUrl")
        count = 0
        while count < batch_size:
            response = await client.receive_message(
                QueueUrl=queue_url,
                WaitTimeSeconds=1,
                MaxNumberOfMessages=1,
            )
            await asyncio.sleep(0.1)
            if "Messages" in response:
                for msg in response["Messages"]:
                    await queue.put(msg["Body"])
                    delete_messages.append(
                        {
                            "Id": msg["MessageId"],
                            "ReceiptHandle": msg["ReceiptHandle"],
                        }  # noqa: E501
                    )
                    count += 1
            else:
                print("No Messages in Queue")
                continue
    except Exception as e:
        print(e)
    else:
        # purging the queue to ensure all the messages are deleted
        # await client.purge_queue(QueueUrl=queue_url)
        for batch in iter(batch_records(delete_messages, 10)):
            await client.delete_message_batch(
                QueueUrl=queue_url, Entries=batch
            )  # noqa: E501
            await asyncio.sleep(0.1)
        return queue


async def push_batch_to_queue(
    client: AioBaseClient,
    queue_name: str,
    batch: List[str],
) -> None:
    """Sends messages to a queue in AWS given by queue_name

    Parameters
    ----------
    client : AioBaseClient
        AWS SQS client
    queue_name : str
        name of the Queue to push messages to
    batch : List[str]
        records in this batch are pushed to queue in AWS

    Raises
    ------
    err
        _description_
    """
    print(f"sending message to queue {queue_name} with batch {batch}")
    AWS_ERROR_MSG = "AWS.SimpleQueueService.NonExistentQueue"
    try:
        response = await client.get_queue_url(QueueName=queue_name)
    except ClientError as err:
        if err.response["Error"]["Code"] == AWS_ERROR_MSG:
            print(f"Queue {queue_name} does not exist")
        else:
            raise
    else:
        queue_url = response["QueueUrl"]
        try:
            entries = [
                {
                    "Id": str(ind),
                    "MessageBody": msg["body"],
                }
                for ind, msg in enumerate(batch)
            ]
            response = await client.send_message_batch(
                QueueUrl=queue_url, Entries=entries
            )
            if "Successful" in response:
                for msg_meta in response["Successful"]:
                    logger.info(
                        "Message sent: %s: %s",
                        msg_meta["MessageId"],
                        batch[int(msg_meta["Id"])]["body"],
                    )
            elif "Failed" in response:
                for msg_meta in response["Failed"]:
                    logger.warning(
                        "Failed to send: %s: %s",
                        msg_meta["MessageId"],
                        batch[int(msg_meta["Id"])]["body"],
                    )
        except ClientError as err:
            logger.exception(f"Send messages failed to queue: {queue_name}")
            raise err


async def send_and_poll(
    client: AioBaseClient,
    batch: List[str],
) -> asyncio.Queue:
    """Sends and polls messages to AWS Queue

    Parameters
    ----------
    client : AioBaseClient
        AWS SQS Client
    batch : List[str]
        batch of records to send to AWS Queue

    Returns
    -------
    asyncio.Queue
        _description_
    """
    await push_batch_to_queue(client, queue_name=MESSAGE_QUEUE, batch=batch)

    # poll the AWS Queue for this particular batch
    message = await poll_queue_for_batch(
        client, queue_name=RESULT_QUEUE, batch_size=len(batch)
    )
    return message


def batch_records(records: List[Any], batch_size: int) -> List[List[Any]]:
    """Batches records by batch size

    Parameters
    ----------
    records : List[Any]
        the records to batch
    batch_size : int
        size of each batch

    Returns
    -------
    List[List[Any]]
        batch of records given by batch_size
    """
    num_batches: int = math.ceil(len(records) / batch_size)

    result = []
    for i in range(num_batches):
        result.append(
            records[i * batch_size : (i + 1) * batch_size]  # noqa: E203
        )  # noqa: E501
    return result


async def handler(event: Dict[str, Any], context=None):
    if event["action"] == "submit_messages":
        session = get_session()
        async with session.create_client("sqs") as client:
            tasks = [
                asyncio.create_task(send_and_poll(client, batch))
                for batch in iter(
                    batch_records(event["messages"], event["batch_size"])
                )  # noqa: E501
            ]
            result = []
            for task in asyncio.as_completed(tasks):
                messages = await task
                # keep processing until queue is empty
                while messages.qsize():
                    msg: Dict[str, List[List[float]]] = json.loads(
                        json.loads(await messages.get())
                    )  # noqa
                    ticker, val = msg.popitem()
                    df = pd.DataFrame(np.array(val).reshape(10, 10))
                    df["ticker"] = ticker
                    result.append(df)
            df = pd.concat(result)
            print(df)
            return df


if __name__ == "__main__":
    # -- Only used for debugging purposes
    start = time.perf_counter()
    messages = [
        {"body": ticker}
        for ticker in [,
            "BTC",
            "ETH",
            "ADA",
            "LUNA",
            "MATIC",
            "POLKADOT",
            "SOL",
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "DODGECOIN",
            "SHIBAINU",
        ]
    ]
    event = {
        "action": "submit_messages",
        "messages": messages,
        "batch_size": 10,
    }
    asyncio.run(handler(event, None))
    end = time.perf_counter()
    print(f"program finished in {(end-start):.4f} seconds")
