import numpy as np

from s2warehouse.ingest import downsample_2x2, retry


def test_downsample_2x2_ignores_nodata():
    a = np.array(
        [[10, 20, 0, 0],
         [30, 40, 0, 0],
         [5, 0, 8, 8],
         [0, 0, 8, 8]],
         dtype="uint16"
    )
    assert downsample_2x2(a).tolist() == [[25, 0], [5, 8]]

def test_retry_eventually_succeeds():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise OSError("transient")
        return "ok"

    assert retry(flaky, attempts=4, base_delay=0) == "ok"
    assert len(calls) == 3