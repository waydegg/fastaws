from datetime import date, datetime
from typing import Any, Literal
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Tag

from fastaws.core import AwsClient
from fastaws.enums import Service

from .models import S3ListObjectsRes, S3Object, S3ObjectOwner

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

    async def list_objects(
        self,
        bucket: str,
        *,
        count: int | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
    ):
        res = await self._make_request(
            method="GET",
            action="ListObjects",
            host=f"{bucket}.{self.host}",
            params={"max-keys": count, "prefix": prefix, "delimiter": delimiter},
        )
        res.raise_for_status()

        soup = BeautifulSoup(res.content.decode(), "xml")

        s3_objects = []
        content_els = soup.find_all("Contents")
        for content_el in content_els:
            s3_object_owner = S3ObjectOwner(
                id=content_el.Owner.ID.text,
                display_name=content_el.Owner.DisplayName.text,
            )
            s3_object = S3Object(
                key=content_el.Key.text,
                last_modified=datetime.strptime(
                    content_el.LastModified.text, "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                etag=content_el.find("ETag").text.strip('"'),
                size=int(content_el.Size.text),
                storage_class=content_el.StorageClass.text.lower(),
                owner=s3_object_owner,
                type=content_el.Type.text.lower() if content_el.Type else None,
            )
            s3_objects.append(s3_object)

        next_marker_el = soup.find("NextMarker")
        next_marker = None
        if isinstance(next_marker_el, Tag):
            next_marker = next_marker_el.text

        s3_list_objets_res = S3ListObjectsRes(
            objects=s3_objects, next_marker=next_marker
        )

        return s3_list_objets_res

    async def put_object(
        self,
        bucket: str,
        *,
        data: Any,
        remote_filepath: str,
        access: AmzAcl = "private",
    ):
        """
        `remote_filepath` must start with a "/"
        """
        f_remote_filepath = urlparse(remote_filepath).path

        res = await self._make_request(
            method="PUT",
            action="PutObject",
            host=f"{bucket}.{self.host}",
            endpoint=f_remote_filepath,
            extra_headers={"x-amz-acl": access},
            data=data,
        )

        return res
