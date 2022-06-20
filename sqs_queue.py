# - the purpose of this file is to send messages to a sqs queue to invoke a lambda function on trigger
# the triggered lambda function computes a covariance matrix that i end up storing in a distributed queue system
# then i obtain the value from the queue for the following tickers

import asyncio
import logging
import math
import time
from typing import Any, List

import uvloop
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/test_queue"
result_queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/result_queue"

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def receive_messages(client, queue_name: str, total_messages: int):
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
        await client.purge_queue(QueueUrl=queue_url)
        return queue


async def send_batch_message(client, queue_name, batch):
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
                    "MessageBody": msg["Body"],
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
                        batch[int(msg_meta["Id"])]["Body"],
                    )
            elif "Failed" in response:
                for msg_meta in response["Failed"]:
                    logger.warning(
                        "Failed to send: %s: %s",
                        msg_meta["MessageId"],
                        batch[int(msg_meta["Id"])]["Body"],
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
    yield from result


async def send_and_receive(client, batch):
    res = await send_batch_message(client, "test_queue", batch)
    return res
    # result = await receive_messages(client, "result_queue")
    # return result


async def handler(event, context=None):
    if event["action"] == "test-queue-process":
        messages = [
            {"Body": ticker}
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
                asyncio.create_task(send_batch_message(client, "test_queue", batch))
                for batch in batch_records(messages, 5)
            ]
            result = await asyncio.gather(*tasks)
            print(result)

            result = await receive_messages(
                client,
                "result_queue",
                len(messages),
            )
            print(result)
            return result


if __name__ == "__main__":
    start = time.perf_counter()
    event = {"action": "test-queue-process"}
    asyncio.run(handler(event, None))
    end = time.perf_counter()
    print(f"program finished in {(end-start):.4f} seconds")
