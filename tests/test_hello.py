from koala import hello


def test_hello_returns_string():
    assert isinstance(hello(), str)
    assert "Hello" in hello()
