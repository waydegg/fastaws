from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class S3ObjectOwner:
    id: str
    display_name: str


@dataclass
class S3Object:
    key: str
    last_modified: datetime
    etag: str
    size: int
    storage_class: str
    owner: S3ObjectOwner
    type: str | None = None


@dataclass
class S3ListObjectsRes:
    objects: List[S3Object]
    next_marker: str | None = None
