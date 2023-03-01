from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Optional

import aiofiles
import httpx

from fastaws.core import AwsClient
from fastaws.enums import Service


class S3Client(AwsClient):
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        region: str,
        provider: Literal["amazonaws", "wasabisys"] = "amazonaws",
    ):
        # host = f"s3.{self.config.region}.{self._domain}.com"
        # if bucket:
        #     host = f"{bucket}.{host}"

        super().__init__(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            service=Service.S3,
            host=f"s3.{region}.{provider}.com",
            version=date(year=2006, month=3, day=1),
        )
        self.provider = provider

    async def list_buckets(self):
        res = await self._make_request(method="GET", action="ListBuckets", endpoint="/")

        return res

    # async def put_object(
    #     self,
    #     *,
    #     bucket: str,
    #     remote_filepath: str,
    #     local_filepath: Optional[str] = None,
    #     data: Optional[Any] = None,
    # ):
    #     """
    #     Example: `await client.put_object(bucket="ooga", remote_filepath="/test.txt", data="meow")`
    #     """
    #     assert self._httpx is not None
    #     if any([local_filepath, data]):
    #         assert not all(
    #             [local_filepath, data]
    #         ), "Specify either `local_filepath` or `data`"
    #
    #     await self._make_request(
    #         method="PUT",
    #         action="PutObject",
    #         uri=remote_filepath,
    #         local_filepath=local_filepath,
    #         data=data,
    #         bucket=bucket,
    #     )
