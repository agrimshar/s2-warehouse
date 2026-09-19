import numpy as np

from s2warehouse.silver import zonal_mean_std
from s2warehouse.storage import scene_id_from_key


def test_zonal_mean_std():
    values = np.array([[1.0, 3.0, 10.0], [2.0, 4.0, 10.0]], dtype="float32")
    cell_index = np.array([[0, 0, 1], [0, 0, -1]])
    valid = cell_index >= 0
    mean, std, cnt = zonal_mean_std(values, cell_index, valid, n=3)
    assert mean[0] == 2.5 and cnt[0] == 4
    assert abs(std[0] - np.sqrt(1.25)) < 1e-6
    assert mean[1] == 10.0 and std[1] == 0.0 and cnt[1] == 1
    assert np.isnan(mean[2]) and cnt[2] == 0


def test_scene_id_from_key():
    key = "bronze/scene_id=S2B_17TPJ_20250704_0_L2A/SCL.tif"
    assert scene_id_from_key(key) == "S2B_17TPJ_20250704_0_L2A"