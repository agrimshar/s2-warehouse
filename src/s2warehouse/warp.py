import pandas as pd
import rasterio
from rasterio.warp import transform as warp_transform

df = pd.read_parquet("manifest.parquet")
href = df.loc[df.scene_id == "S2B_17TPJ_20250704_0_L2A", "href_B04"].item()
with rasterio.open(href) as src:
    x, y = src.transform * (5000 + 256, 5000 + 256)  # pixel (col, row) -> metres
    lon, lat = warp_transform(src.crs, "EPSG:4326", [x], [y])
print(lat[0], lon[0])