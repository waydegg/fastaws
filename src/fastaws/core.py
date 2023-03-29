import json
import urllib.parse as urllib
from datetime import date, datetime
from typing import Dict

from httpx import AsyncClient, Response
from structlog import get_logger

from .auth import get_hash, get_signature, get_signature_key
from .enums import Service

logger = get_logger()


class AwsClient:
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        region: str,
        service: Service,
        host: str,
        version: date,
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service
        self.host = host
        self.version = version

        self._httpx = None

    async def connect(self):
        assert self._httpx is None, "AwsClient already connected"
        self._httpx = AsyncClient(timeout=None)

    async def disconnect(self):
        assert self._httpx is not None, "AwsClient is not connected"
        await self._httpx.aclose()
        self._httpx = None

    async def _make_request(
        self,
        *,
        method: str,
        action: str,
        host: str | None = None,
        endpoint: str = "/",
        params: Dict | None = None,
        extra_headers: Dict | None = None,
        data: Dict | None = None,
    ) -> Response:
        assert isinstance(self._httpx, AsyncClient)

        host = host or self.host

        utcnow = datetime.utcnow()
        amz_date = utcnow.strftime("%Y%m%dT%H%M%SZ")
        datestamp = utcnow.strftime("%Y%m%d")

        canonical_querystring_parts = [
            f"Action={action}",
            f"Version={self.version.strftime('%Y-%m-%d')}",
        ]
        if params:
            for k, v in params.items():
                if v is None:
                    continue
                if isinstance(v, str):
                    value_querystring = urllib.quote(v, safe="")
                else:
                    value_querystring = v
                item_querystring = f"{k}={value_querystring}"
                canonical_querystring_parts.append(item_querystring)
        canonical_querystring = "&".join(sorted(canonical_querystring_parts))

        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-date"

        payload = None
        if data is not None:
            if isinstance(data, dict):
                payload = json.dumps(data)
            elif isinstance(data, bytes):
                payload = data
        payload_hash = get_hash("" if payload is None else payload)

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
        credential_scope = (
            f"{datestamp}/{self.region}/{self.service.value}/aws4_request"
        )

        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}"
        )
        signature_key = get_signature_key(
            key=self.secret_key,
            datestamp=datestamp,
            region=self.region,
            service=self.service,
        )
        signature = get_signature(
            signature_key=signature_key, string_to_sign=string_to_sign
        )

        authorization_header_parts = [
            algorithm,
            f"Credential={self.access_key}/{credential_scope},",
            f"SignedHeaders={signed_headers},",
            f"Signature={signature}",
        ]
        authorization_header = " ".join(authorization_header_parts)

        headers = {
            "x-amz-date": amz_date,
            "Authorization": authorization_header,
            "x-amz-content-sha256": payload_hash,
        }
        if extra_headers:
            headers.update(extra_headers)

        res = await self._httpx.request(
            method=method,
            url=f"https://{host}{endpoint}?{canonical_querystring}",
            headers=headers,
            content=payload,
        )

        return res
