import hashlib
import hmac
from datetime import datetime
from typing import Any, Dict, Literal, Optional

import aiofiles

from .enums import Service


def get_signature_key(*, key: str, datestamp: str, region: str, service: Service):
    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    signature_key = sign(("AWS4" + key).encode("utf-8"), datestamp)
    signature_key = sign(signature_key, region)
    signature_key = sign(signature_key, service.value)
    signature_key = sign(signature_key, "aws4_request")

    return signature_key


def get_signature(*, signature_key: bytes, string_to_sign: str):
    return hmac.new(
        signature_key, (string_to_sign).encode("utf-8"), hashlib.sha256
    ).hexdigest()


def get_hash(value: str):
    return hashlib.sha256((value).encode("utf-8")).hexdigest()


async def get_file_hash(filepath: str):
    file_hash = hashlib.sha256()

    async with aiofiles.open(filepath, "rb") as f:
        while chunk := await f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()
