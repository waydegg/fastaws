from datetime import datetime
from typing import Literal

from httpx import AsyncClient

from .auth import get_hash, get_signature, get_signature_key


class AwsClient:
    def __init__(
        self,
        *,
        aws_access_key: str,
        aws_secret_key: str,
        region: str,
        service: Literal["s3", "ses"],
        **kwargs: str,
    ):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.service = service

        match (service):
            case "s3":
                raise NotImplementedError()
            case "ses":
                self.host = f"email.{self.region}.amazonaws.com"

        self._httpx = None

    async def connect(self):
        assert self._httpx is None, "AwsClient already connected"
        self._httpx = AsyncClient()

    async def disconnect(self):
        assert self._httpx is not None, "AwsClient is not connected"
        await self._httpx.aclose()
        self._httpx = None

    async def _make_request(
        self, *, method: str, endpoint: str, action: str, data: str | None = None
    ):
        assert isinstance(self._httpx, AsyncClient)

        utcnow = datetime.utcnow()
        amz_date = utcnow.strftime("%Y%m%dT%H%M%SZ")
        datestamp = utcnow.strftime("%Y%m%d")

        request_parameters = f"Action={action}&Version=2013-10-15"
        canonical_querystring = request_parameters

        canonical_headers = f"host:{self.host}\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-date"

        # TODO: handle data and files
        payload_hash = get_hash("")

        canonical_request_parts = [
            method,
            endpoint,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
        canonical_request = "\n".join(canonical_request_parts)
        hashed_canonical_request = get_hash(canonical_request)

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{datestamp}/{self.region}/s3/aws4_request"

        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}"
        )
        signature_key = get_signature_key(
            key=self.aws_secret_key,
            datestamp=datestamp,
            region=self.region,
            service="s3",
        )
        signature = get_signature(
            signature_key=signature_key, string_to_sign=string_to_sign
        )

        authorization_header_parts = [
            algorithm,
            f"Credential={self.aws_access_key}/{credential_scope},",
            f"SignedHeaders={signed_headers},",
            f"Signature={signature}",
        ]
        authorization_header = " ".join(authorization_header_parts)

        headers = {
            "x-amz-date": amz_date,
            "Authorization": authorization_header,
            "x-amz-content-sha256": payload_hash,
        }

        res = await self._httpx.request(
            method=method,
            url=f"https://{self.host}{endpoint}?{canonical_querystring}",
            headers=headers,
            content=data or None,
        )
        res.raise_for_status()

        return res
