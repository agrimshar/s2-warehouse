import numpy as np

from s2warehouse.raster import ndvi


def test_ndvi_known_values():
    red = np.array([[100, 200], [0, 300]], dtype="uint16")
    nir = np.array([[300, 200], [0, 100]], dtype="uint16")
    v = ndvi(red, nir)
    assert v[0, 0] == 0.5
    assert v[0, 1] == 0.0
    assert np.isnan(v[1, 0])
    assert v[1, 1] == -0.5