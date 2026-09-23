'''Query Earth Search (STAC) for Sentinel-2 L2A scenes and build a manifest'''

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from pystac_client import Client

from s2warehouse.metrics import timed
from s2warehouse.storage import BUCKET, upload

STAC_URL = "https://earth-search.aws.element84.com/v1"
COLLECTION = "sentinel-2-l2a"
TILE = "MGRS-17TPJ"

# earth search asset key -> sentinel 2 band name
BANDS = {"green": "B03", "red": "B04", "nir": "B08", "scl": "SCL"}

# gta bbox. lon_min, lat_min, lon_max, lat_max
GTA_BBOX = [-79.9, 43.45, -79.0, 44.1]

def item_to_row(item) -> dict:
    """Flatten one STAC item into one manifest row"""
    p = item.properties
    row = {
        "scene_id": item.id,
        "datetime": p["datetime"],
        "tile": p.get("grid:code"),
        "cloud_cover": p.get("eo:cloud_cover"),
        "nodata_pct": p.get("s2:nodata_pixel_percentage"),
    }
    for asset_key, band in BANDS.items():
        row[f"href_{band}"] = item.assets[asset_key].href
    return row

def search_scenes(bbox, start, end, max_cloud=80, tile=TILE) -> list[dict]:
    client = Client.open(STAC_URL)
    search = client.search(
        collections=[COLLECTION],
        bbox=bbox,
        datetime=f"{start}/{end}",
        query={
            "eo:cloud_cover": {"lt": max_cloud},
            "grid:code": {"eq": tile},
        },
    )
    return [item_to_row(item) for item in search.items()]

def build_manifest(bbox=GTA_BBOX, start="2025-01-01", end=None, max_cloud=80):
    end = end or datetime.now(tz=UTC).date().isoformat()
    df = pd.DataFrame(search_scenes(bbox, start, end, max_cloud))
    df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601", utc=True)
    return df.sort_values("datetime").reset_index(drop=True)

if __name__ == "__main__":
    with timed("catalog") as m:
        df = build_manifest()
        m["scenes"] = len(df)
        print(f"{len(df)} scenes")
        print()
        print("scenes per tile:")
        print(df.groupby("tile").size())
        print()
        print("cloud cover:")
        print(df["cloud_cover"].describe())
        print()
        print("nodata:")
        print(df["nodata_pct"].describe())
        df.to_parquet("manifest.parquet", index=False)
        print()
        print("wrote manifest.parquet")

        key = "bronze/manifest/manifest.parquet"
        upload(Path("manifest.parquet"), key)
        print(f"uploaded s3://{BUCKET}/{key}")
    