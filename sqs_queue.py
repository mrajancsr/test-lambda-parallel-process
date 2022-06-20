# The file computes the covariance matrix by sending messages as tickers to a
# distributed queue in AWS which triggers a lambda function concurrently
# each triggered lambda function is running a multi-processing for
# batch of tickers

import asyncio
import logging
import math
import time
from typing import Any, List

import uvloop
from aiobotocore.session import AioBaseClient, get_session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

MESSAGE_QUEUE = "test_queue"
RESULT_QUEUE = "result_queue"

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def receive_messages(
    client: AioBaseClient,
    queue_name: str,
    total_messages: int,
) -> asyncio.Queue:
    print(f"receiving message from {queue_name}")
    queue = asyncio.Queue()
    try:
        response = await client.get_queue_url(QueueName=queue_name)
        queue_url = response.get("QueueUrl")
        count = 0
        while count < total_messages:
            response = await client.receive_message(
                QueueUrl=queue_url,
                WaitTimeSeconds=0,
                MaxNumberOfMessages=10,
            )
            if "Messages" in response:
                for msg in response["Messages"]:
                    await queue.put(msg["Body"])
                    await client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
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
        return queue


async def send_batch_message(
    client: AioBaseClient,
    queue_name: str,
    batch: List[str],
):
    print(f"sending message to queue {queue_name} with batch {batch}")
    try:
        response = await client.get_queue_url(QueueName=queue_name)
    except ClientError as err:
        if err.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
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
        else:
            return response


def batch_records(records: List[Any], batch_size: int) -> List[List[Any]]:
    num_batches: int = math.ceil(len(records) / batch_size)

    result = []
    for i in range(num_batches):
        result.append(records[i * batch_size : (i + 1) * batch_size])
    return result


async def handler(event, context=None):
    if event["action"] == "submit_messages":
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
        session = get_session()
        async with session.create_client("sqs", region_name="us-east-1") as client:
            tasks = [
                asyncio.create_task(send_batch_message(client, MESSAGE_QUEUE, batch))
                for batch in iter(batch_records(messages, 5))
            ]
            result = await asyncio.gather(*tasks)
            print(result)

            result = await receive_messages(
                client,
                RESULT_QUEUE,
                len(messages),
            )
            print(result)
            return result


if __name__ == "__main__":
    # Only used for debugging purposes
    start = time.perf_counter()
    event = {"action": "submit_messages"}
    asyncio.run(handler(event, None))
    end = time.perf_counter()
    print(f"program finished in {(end-start):.4f} seconds")
