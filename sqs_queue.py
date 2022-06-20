# - the purpose of this file is to send messages to a sqs queue to invoke a lambda function on trigger
# the triggered lambda function computes a covariance matrix that i end up storing in a distributed queue system
# then i obtain the value from the queue for the following tickers

import asyncio
import logging
import math
from typing import Any, List

from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from parallel_handler import receive_message

logger = logging.getLogger(__name__)

queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/test_queue"
result_queue_url = "https://sqs.us-east-1.amazonaws.com/955157183814/result_queue"


async def receive_messages(client, queue_name):
    result = []
    try:
        response = await client.get_queue_url(QueueName=queue_name)
        queue_url = response.get("QueueUrl")
        while True:
            response = await client.receive_message(
                QueueUrl=queue_url, WaitTimeSeconds=2
            )
            if "Messages" in response:
                for msg in response["Messages"]:
                    print(f"Got msg {msg['Body']}")
                    result.append(msg["Body"])
                    await client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
            else:
                print("No Messages in Queue")
                break
    except Exception as e:
        print(e)
    else:
        return result


async def send_batch_message(client, queue_name):
    pass


def batch_records(records: List[Any], batch_size: int) -> List[List[Any]]:
    num_batches: int = math.ceil(len(records) / batch_size)

    result = []
    for i in range(num_batches):
        result.append(records[i * batch_size : (i + 1) * batch_size])
    yield from result


async def send_and_receive(client, batch):
    await send_batch_message(client, "test_queue")
    result = await receive_message()
    return result


async def handler(event, context=None):
    if event["action"] == "test-queue-process":
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
                asyncio.create_task(send_and_receive(client, batcn))
                for batcn in batch_records(messages, 10)
            ]
            result = await asyncio.gather(*tasks)
            print(result)


if __name__ == "__main__":

    session = get_session()
