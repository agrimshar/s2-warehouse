from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "us-west-2"
BUCKET = "s2-warehouse-agrim-2026"

def bronze_key(scene_id: str, band: str) -> str:
    """Object key for on band of scene in the bronze layer"""
    return f"bronze/scene_id={scene_id}/{band}.tif"

def object_exists(key: str) -> bool:
    s3 = boto3.client("s3", region_name = REGION)
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True

def upload(local: Path, key: str) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(str(local), BUCKET, key)

def download(key: str, local: Path) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.download_file(BUCKET, key, str(local))

def scene_id_from_key(key: str) -> str:
    return key.split("scene_id=")[1].split("/")[0]


def list_complete_scene_ids() -> list[str]:
    """Scenes with all four bands in bronze. SCL is written last, so it is the marker."""
    s3 = boto3.client("s3", region_name=REGION)
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix="bronze/scene_id="
    )
    ids = [
        scene_id_from_key(obj["Key"])
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith("/SCL.tif")
    ]
    return sorted(ids)