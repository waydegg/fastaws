from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

import aiofiles
import httpx

from fastaws.auth import (get_file_hash, get_hash, get_signature,
                          get_signature_key)


@dataclass
class S3Config:
    provider: Literal["wasabi"] | Literal["amazon"]
    access_key: str
    secret_key: str
    region: str


class S3Client:
    def __init__(self, config: S3Config):
        self.config = config
        self._httpx = None

        match self.config.provider:
            case "wasabi":
                self._domain = "wasabisys"
            case "amzon":
                self._domain = "amazonaws"

    async def connect(self):
        assert self._httpx is None, "S3 already connected"
        self._httpx = httpx.AsyncClient()

    async def disconnect(self):
        assert self._httpx is not None, "S3 is not connected"
        await self._httpx.aclose()
        self._httpx = None

    async def _make_request(
        self,
        *,
        method: str,
        action: str,
        bucket: Optional[str] = None,
        uri: str = "/",
        local_filepath: Optional[str] = None,
        data: Optional[str] = None,
    ):
        assert self._httpx is not None

        utcnow = datetime.utcnow()
        amz_date = utcnow.strftime("%Y%m%dT%H%M%SZ")
        datestamp = utcnow.strftime("%Y%m%d")

        request_parameters = f"Action={action}&Version=2013-10-15"
        canonical_querystring = request_parameters

        host = f"s3.{self.config.region}.{self._domain}.com"
        if bucket:
            host = f"{bucket}.{host}"

        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-date"

        if data:
            payload_hash = get_hash(data)
        elif local_filepath:
            payload_hash = await get_file_hash(local_filepath)
        else:
            payload_hash = get_hash("")

        canonical_request_parts = [
            method,
            uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
        canonical_request = "\n".join(canonical_request_parts)
        hashed_canonical_request = get_hash(canonical_request)

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{datestamp}/{self.config.region}/s3/aws4_request"

        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}"
        )
        signature_key = get_signature_key(
            key=self.config.secret_key,
            datestamp=datestamp,
            region=self.config.region,
            service="s3",
        )
        signature = get_signature(
            signature_key=signature_key, string_to_sign=string_to_sign
        )

        authorization_header_parts = [
            algorithm,
            f"Credential={self.config.access_key}/{credential_scope},",
            f"SignedHeaders={signed_headers},",
            f"Signature={signature}",
        ]
        authorization_header = " ".join(authorization_header_parts)

        headers = {
            "x-amz-date": amz_date,
            "Authorization": authorization_header,
            "x-amz-content-sha256": payload_hash,
        }

        if local_filepath:
            # async with aiofiles.open(local_filepath, "rb") as f:
            #     res = await self._httpx.request(
            #         method=method,
            #         url=f"https://{host}{uri}?{canonical_querystring}",
            #         headers=headers,
            #         # content=f,
            #         files = {"upload-file": f}
            #     )

            res = await self._httpx.request(
                method=method,
                url=f"https://{host}{uri}?{canonical_querystring}",
                headers=headers,
                # content=f,
                files={"upload-file": open(local_filepath, "rb")},
            )
        else:
            res = await self._httpx.request(
                method=method,
                url=f"https://{host}{uri}?{canonical_querystring}",
                headers=headers,
                content=data or None,
            )
        res.raise_for_status()

        return res

    async def list_buckets(self):
        assert self._httpx is not None
        res = await self._make_request(method="GET", action="ListBuckets")

        return res

    async def put_object(
        self,
        *,
        bucket: str,
        remote_filepath: str,
        local_filepath: Optional[str] = None,
        data: Optional[Any] = None,
    ):
        """
        Example: `await client.put_object(bucket="ooga", remote_filepath="/test.txt", data="meow")`
        """
        assert self._httpx is not None
        if any([local_filepath, data]):
            assert not all(
                [local_filepath, data]
            ), "Specify either `local_filepath` or `data`"

        await self._make_request(
            method="PUT",
            action="PutObject",
            uri=remote_filepath,
            local_filepath=local_filepath,
            data=data,
            bucket=bucket,
        )
