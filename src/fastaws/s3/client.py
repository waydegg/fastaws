from datetime import date, datetime
from typing import Any, Literal

from bs4 import BeautifulSoup, Tag
from ipdb import set_trace

from fastaws.core import AwsClient
from fastaws.enums import Service

AmzAcl = (
    Literal["private"]
    | Literal["public-read"]
    | Literal["public-read-write"]
    | Literal["authenticated-read"]
    | Literal["aws-exec-read"]
    | Literal["bucket-owner-read"]
    | Literal["bucket-owner-full-control"]
)


class S3Client(AwsClient):
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        region: str,
        provider: Literal["amazonaws", "wasabisys", "digitaloceanspaces"],
    ):
        match provider:
            case "digitaloceanspaces":
                host = f"{region}.{provider}.com"
            case _:
                host = f"s3.{region}.{provider}.com"

        super().__init__(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            service=Service.S3,
            host=host,
            version=date(year=2006, month=3, day=1),
        )
        self.provider = provider

    async def list_buckets(self):
        res = await self._make_request(method="GET", action="ListBuckets")
        res.raise_for_status()

        buckets = []

        soup = BeautifulSoup(res.content.decode(), "xml")
        bucket_els = soup.find_all("Bucket")
        for bucket_el in bucket_els:
            name_el = bucket_el.find("Name")
            assert isinstance(name_el, Tag)
            name = name_el.text

            creation_date_el = bucket_el.find("CreationDate")
            assert isinstance(creation_date_el, Tag)
            creation_date = datetime.strptime(
                creation_date_el.text, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            bucket_data = {"name": name, "creation_date": creation_date}
            buckets.append(bucket_data)

        return buckets

    async def list_objects(self, bucket: str):
        res = await self._make_request(
            method="GET", action="ListObjects", host=f"{bucket}.{self.host}"
        )
        res.raise_for_status()

        data = res.content.decode()

        return data

    async def put_object(
        self,
        bucket: str,
        *,
        data: Any,
        remote_filepath: str,
        access: AmzAcl = "private",
    ):
        res = await self._make_request(
            method="PUT",
            action="PutObject",
            host=f"{bucket}.{self.host}",
            endpoint=remote_filepath,
            extra_headers={"x-amz-acl": access},
            data=data,
        )
        return res
