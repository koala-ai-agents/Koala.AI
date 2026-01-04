from koala.auth import default_auth
from koala.tools import ToolsError, ToolsRegistry


def test_auth_manager_allows_and_denies():
    auth = default_auth
    # register an api key with permission for 'foo'
    auth.register_api_key("key1", allowed_tools=["foo"])

    reg = ToolsRegistry()

    def foo():
        return "ok"

    reg.register("foo", foo)

    # allowed principal should succeed
    assert reg.call("foo", principal="key1") == "ok"

    # unknown principal should be denied
    try:
        reg.call("foo", principal="nope")
        raise AssertionError("Expected ToolsError for unauthorized principal")
    except ToolsError:
        pass
