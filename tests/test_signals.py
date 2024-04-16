def additional_fuck(x=0, y=0):
    return x + y


def test_additional():
    assert additional_fuck(2, 2) == 4
