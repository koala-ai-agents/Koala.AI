from koala.tools import ToolsError, ToolsRegistry, default_registry, tool


def test_registry_register_and_call():
    reg = ToolsRegistry()

    def add(a: int, b: int):
        return a + b

    reg.register("add", add, input_schema={"a": "int", "b": "int"})
    assert "add" in reg.list_tools()
    res = reg.call("add", a=2, b=3)
    assert res == 5


def test_registry_type_validation():
    reg = ToolsRegistry()

    def join(s1: str, s2: str):
        return s1 + s2

    reg.register("join", join, input_schema={"s1": "str", "s2": "str"})
    try:
        reg.call("join", s1="a", s2=1)  # wrong type
        raise AssertionError("expected ToolsError due to wrong type")
    except ToolsError:
        pass


def test_decorator_registers_tool_and_default_registry():
    # use default_registry via decorator

    @tool("echo", input_schema={"event": "str", "state": "str"})
    def echo(event: str, state: str):
        return f"{state}:{event}"

    assert "echo" in default_registry.list_tools()
    out = default_registry.call("echo", event="go", state="start")
    assert out == "start:go"
