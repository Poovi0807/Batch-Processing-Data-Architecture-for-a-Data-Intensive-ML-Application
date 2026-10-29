from conftest import SAMPLE
from scripts.split_by_month import split


def test_split_real_sample_by_month(tmp_path):
    counts = split(SAMPLE, tmp_path)
    assert counts == {"2005-06": 498, "2005-07": 701, "2005-08": 179, "2005-09": 95,
                      "2005-10": 53, "2005-11": 280, "2005-12": 193, "2006-01": 1}
    assert sum(len(p.read_bytes().splitlines()) for p in tmp_path.glob("*.log")) == 2000
