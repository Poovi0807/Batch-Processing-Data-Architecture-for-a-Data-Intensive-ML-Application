import pytest

from jobs.periods import is_quarter_end, months_of_quarter, quarter_of


def test_quarter_of():
    assert quarter_of("2005-06") == "2005Q2"
    assert quarter_of("2005-07") == "2005Q3"
    assert quarter_of("2006-01") == "2006Q1"


def test_months_of_quarter():
    assert months_of_quarter("2005Q3") == ["2005-07", "2005-08", "2005-09"]
    assert months_of_quarter("2005Q4") == ["2005-10", "2005-11", "2005-12"]


def test_is_quarter_end():
    assert [m for m in range(1, 13) if is_quarter_end(f"2005-{m:02d}")] == [3, 6, 9, 12]


@pytest.mark.parametrize("bad", ["2005-13", "05-06", "2005/06", "", "2005Q5"])
def test_bad_input_is_refused(bad):
    with pytest.raises(ValueError):
        quarter_of(bad)
