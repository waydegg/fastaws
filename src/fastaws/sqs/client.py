import json
from datetime import date
from typing import Any, Dict, List

from structlog import get_logger

from fastaws.core import AwsClient
from fastaws.enums import Service

from .models import (SqsGetQueuesResponse, SqsReceiveMessageResponse,
                     SqsSendMessageResponse)
from .utils import get_endpoint_from_url

logger = get_logger()


class SqsClient(AwsClient):
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        region: str,
    ):
        super().__init__(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            service=Service.SQS,
            host=f"sqs.{region}.amazonaws.com",
            version=date(year=2012, month=11, day=5),
        )

    async def _make_request(
        self,
        *,
        method: str,
        action: str,
        endpoint: str = "/",
        params: Dict | None = None,
        data: Dict | None = None,
    ) -> Dict[str, Any] | None:
        res = await super()._make_request(
            method=method,
            action=action,
            endpoint=endpoint,
            params=params,
            extra_headers={"Accept": "application/json"},
        )
        data = json.loads(res.content.decode())

        if data is not None and "Error" in data:
            logger.error(
                "HttpRequest error",
                status_code=res.status_code,
                reason=res.reason_phrase,
                aws_code=data["Error"]["Code"],
                aws_message=data["Error"]["Message"],
            )
            return

        return data

    async def get_queue(self, name: str) -> str | None:
        data = await self._make_request(
            method="GET", action="GetQueueUrl", params={"QueueName": name}
        )
        if data is None:
            return
        queue_url = data["GetQueueUrlResponse"]["GetQueueUrlResult"]["QueueUrl"]
        return queue_url

    async def list_queues(self, prefix: str | None = None):
        data = await self._make_request(
            method="GET",
            action="ListQueues",
            params={"QueueNamePrefix": prefix},
        )
        if data is None:
            return
        get_queues_res = SqsGetQueuesResponse(
            queue_urls=data["ListQueuesResponse"]["ListQueuesResult"]["queueUrls"],
            next_token=data["ListQueuesResponse"]["ListQueuesResult"]["NextToken"],
        )
        return get_queues_res

    async def create_queue(
        self,
        name: str,
        *,
        wait_seconds: int | None = None,
        delay_seconds: int | None = None,
    ) -> str | None:
        attribute_params = {
            "ReceiveMessageWaitTimeSeconds": wait_seconds,
            "DelaySeconds": delay_seconds,
        }
        attribute_params_non_none = {}
        for k, v in attribute_params.items():
            if v is None:
                continue
            attribute_params_non_none[k] = v
        attribute_params_formatted = {}
        for i, (k, v) in enumerate(attribute_params_non_none.items(), start=1):
            attribute_param_formatted = {
                f"Attribute.{i}.Name": k,
                f"Attribute.{i}.Value": v,
            }
            attribute_params_formatted.update(attribute_param_formatted)

        data = await self._make_request(
            method="POST",
            action="CreateQueue",
            params={"QueueName": name, **attribute_params_formatted},
        )
        if data is None:
            return
        queue_url = data["CreateQueueResponse"]["CreateQueueResult"]["QueueUrl"]

        return queue_url

    async def send_message(self, queue_url: str, *, message_body: str | Dict):
        data = await self._make_request(
            method="POST",
            endpoint=get_endpoint_from_url(queue_url),
            action="SendMessage",
            params={"MessageBody": message_body},
        )
        if data is None:
            return

        send_message_response = SqsSendMessageResponse(
            message_id=data["SendMessageResponse"]["SendMessageResult"]["MessageId"]
        )

        return send_message_response

    async def get_messages(
        self,
        queue_url: str,
        *,
        wait_seconds: int | None = None,
        max_messages: int | None = None,
    ) -> List[SqsReceiveMessageResponse]:
        data = await self._make_request(
            method="GET",
            endpoint=get_endpoint_from_url(queue_url),
            action="ReceiveMessage",
            params={
                "WaitTimeSeconds": wait_seconds,
                "MaxNumberOfMessages": max_messages,
            },
        )
        if data is None:
            return []
        messages = data["ReceiveMessageResponse"]["ReceiveMessageResult"]["messages"]
        if messages is None:
            return []

        receive_messages_response = []
        for message in messages:
            receive_message_response = SqsReceiveMessageResponse(
                message_id=message["MessageId"],
                receipt_handle=message["ReceiptHandle"],
                body=message["Body"],
            )
            receive_messages_response.append(receive_message_response)

        return receive_messages_response

    async def delete_message(self, queue_url: str, *, receipt_handle: str):
        await self._make_request(
            method="POST",
            endpoint=get_endpoint_from_url(queue_url),
            action="DeleteMessage",
            params={"ReceiptHandle": receipt_handle},
        )

    async def delete_queue(self, queue_url: str):
        """
        When you delete a queue, the deletion process takes up to 60 seconds.
        Requests you send involving that queue during the 60 seconds might succeed.

        When you delete a queue, you must wait at least 60 seconds before creating a
        queue with the same name.
        """
        return await self._make_request(
            method="POST",
            endpoint=get_endpoint_from_url(queue_url),
            action="DeleteQueue",
        )
