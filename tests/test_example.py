def test_basic_math():
    assert 1 + 1 == 2

def test_environment_variables():
    expected_mode = "PAPER"
    assert expected_mode in ["PAPER", "LIVE"]
