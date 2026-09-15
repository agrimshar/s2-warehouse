from s2warehouse.storage import bronze_key


def test_bronze_key_layout():
    key = bronze_key("S2B_17TPJ_20250704_0_L2A", "B04")
    assert key == "bronze/scene_id=S2B_17TPJ_20250704_0_L2A/B04.tif"