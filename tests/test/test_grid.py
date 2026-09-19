import numpy as np
from rasterio import Affine

from s2warehouse.grid import cells_covering, zonal_fraction


def test_cells_covering_tile_17tpj():
    meta = {
        "crs": "EPSG:32617",
        "transform": Affine(20, 0, 600000, 0, -20, 4900020),
        "shape": (5490, 5490),
    }
    cells = cells_covering(meta)
    assert 2000 < len(cells) < 3500
    assert all(len(c) == 15 for c in cells)


def test_zonal_fraction():
    cell_index = np.array([[0, 0, 1, 1], [0, 0, 1, -1]])
    mask = np.array([[1, 0, 1, 1], [0, 0, 1, 1]], dtype=bool)
    valid = cell_index >= 0
    out = zonal_fraction(cell_index, mask, valid, n=3)
    assert out[0] == 0.25
    assert out[1] == 1.0
    assert np.isnan(out[2])