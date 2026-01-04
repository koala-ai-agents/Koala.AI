from koala.flow import DAGFlow, LocalExecutor, Step
from koala.ingress import IngressError, process_rest_payload
from koala.tools import default_registry, tool


def test_process_payload_with_inline_flow_and_registry():
    # register a tool in default_registry
    @tool("add", input_schema={"a": "int", "b": "int"})
    def add(a: int, b: int):
        return a + b

    flow = DAGFlow(id="r1")
    flow.add_step(Step(id="s1", action="add", args={"a": 2, "b": 3}))

    payload = {"flow": flow.to_dict()}
    res = process_rest_payload(
        payload, registry=default_registry, executor=LocalExecutor()
    )
    assert res["results"]["s1"] == 5


def test_process_payload_missing_repo_for_flow_id_raises():
    payload = {"flow_id": "doesnotexist"}
    try:
        process_rest_payload(payload)
        raise AssertionError("expected IngressError")
    except IngressError:
        pass
