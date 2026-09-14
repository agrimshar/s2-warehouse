from types import SimpleNamespace

from s2warehouse.catalog import item_to_row


def make_item():
    assets = {
        k: SimpleNamespace(href=f"https://example.com/{k}.tif")
        for k in ["green", "red", "nir", "scl"]
    }
    return SimpleNamespace(
        id="S2A_17TPJ_20250701_0_L2A",
        properties={
            "datetime": "2025-07-01T16:00:00Z",
            "grid:code": "MGRS-17TPJ",
            "eo:cloud_cover": 12.5,
            "s2:nodata_pixel_percentage": 3.2,
        },
        assets=assets
    )

def test_item_to_row_extracts_all_fields():
    row = item_to_row(make_item())
    assert row["scene_id"] == "S2A_17TPJ_20250701_0_L2A"
    assert row["tile"] == "MGRS-17TPJ"
    assert row["cloud_cover"] == 12.5
    assert row["nodata_pct"] == 3.2
    assert row["href_B04"].endswith("red.tif")
    hrefs = {k for k in row if k.startswith("href_")}
    assert hrefs == {"href_B03", "href_B04", "href_B08", "href_SCL"}