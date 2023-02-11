from dataclasses import dataclass
from typing import List


@dataclass
class SqsSendMessageResponse:
    message_id: str
    sequence_number: str | None = None


@dataclass
class SqsReceiveMessageResponse:
    message_id: str
    receipt_handle: str
    body: str


@dataclass
class SqsGetQueuesResponse:
    queue_urls: List[str]
    next_token: str | None
