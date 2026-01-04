import time

from koala.flow import DAGFlow, FlowRepository, LocalExecutor, State, StateMachine, Step


def test_dagflow_basic_run():
    # simple registry of actions
    def add(a, b):
        return a + b

    def mul(x, y):
        return x * y

    registry = {"add": add, "mul": mul}

    flow = DAGFlow(id="f1")
    flow.add_step(Step(id="s1", action="add", args={"a": 1, "b": 2}))
    flow.add_step(Step(id="s2", action="mul", args={"x": "$result.s1", "y": 10}))
    flow.add_edge("s1", "s2")

    results = flow.run(registry)
    assert results["s1"] == 3
    assert results["s2"] == 30


def test_serialization_roundtrip():
    flow = DAGFlow(id="f2", version="0.2")
    flow.add_step(Step(id="a", action="noop", args={}))
    flow.add_step(Step(id="b", action="noop", args={}))
    flow.add_edge("a", "b")

    s = flow.dumps()
    new = DAGFlow.loads(s)
    assert new.id == flow.id
    assert new.version == flow.version
    assert len(new.steps) == 2
    assert tuple(new.edges) == tuple(flow.edges)


def test_state_machine_and_repo(tmp_path):
    # create a state machine
    sm = StateMachine(id="sm1")
    sm.add_state(State(id="start", action="echo", on={"go": "end"}))
    sm.add_state(State(id="end", action="echo", on={}))

    def echo(event, state):
        return f"{state}:{event}"

    registry = {"echo": echo}

    results = sm.run(registry, ["go"])
    assert "start" in results
    assert results["start"] == "start:go"

    # test repository save/load using sqlite
    db = tmp_path / "flows.db"
    repo = FlowRepository(str(db))
    repo.save_flow(sm)
    loaded = repo.load_flow("sm1")
    assert isinstance(loaded, StateMachine)
    assert loaded.id == sm.id


def test_local_executor_basic():
    def add(a, b):
        return a + b

    def mul(x, y):
        return x * y

    registry = {"add": add, "mul": mul}

    flow = DAGFlow(id="fe1")
    flow.add_step(Step(id="s1", action="add", args={"a": 2, "b": 3}))
    flow.add_step(Step(id="s2", action="mul", args={"x": "$result.s1", "y": 5}))
    flow.add_edge("s1", "s2")

    ex = LocalExecutor(max_workers=2)
    results = ex.run_dagflow(flow, registry)
    assert results["s1"] == 5
    assert results["s2"] == 25


def test_local_executor_parallelism():
    # two independent steps that sleep; they should run in parallel
    def slow_const(name, delay=0.3):
        time.sleep(delay)
        return name

    registry = {
        "a": lambda: slow_const("a", delay=0.35),
        "b": lambda: slow_const("b", delay=0.35),
        "c": lambda x, y: (x, y),
    }

    f = DAGFlow(id="fp")
    f.add_step(Step(id="a", action="a", args={}))
    f.add_step(Step(id="b", action="b", args={}))
    f.add_step(Step(id="c", action="c", args={"x": "$result.a", "y": "$result.b"}))
    f.add_edge("a", "c")
    f.add_edge("b", "c")

    ex = LocalExecutor(max_workers=2)
    t0 = time.time()
    results = ex.run_dagflow(f, registry)
    elapsed = time.time() - t0
    # serial would take ~0.7s; running in parallel should be noticeably less than sum
    assert elapsed < 0.7
    assert results["c"] == ("a", "b")


def test_dummy_remote_executor_simulated():
    # simulate remote calls with a small artificial delay
    def add(a, b):
        return a + b

    def mul(x, y):
        return x * y

    registry = {"add": add, "mul": mul}

    flow = DAGFlow(id="rf1")
    flow.add_step(Step(id="s1", action="add", args={"a": 4, "b": 6}))
    flow.add_step(Step(id="s2", action="mul", args={"x": "$result.s1", "y": 2}))
    flow.add_edge("s1", "s2")

    from koala.flow import DummyRemoteExecutor

    rex = DummyRemoteExecutor(per_call_delay=0.05)
    results = rex.run_dagflow(flow, registry)
    assert results["s1"] == 10
    assert results["s2"] == 20


def test_flow_builder_basic():
    # builder / DSL should create equivalent flow
    def add(a, b):
        return a + b

    def mul(x, y):
        return x * y

    registry = {"add": add, "mul": mul}

    from koala.flow import dag

    flow = (
        dag("fb")
        .step("s1", "add", a=3, b=4)
        .step("s2", "mul", x="$result.s1", y=2)
        .edge("s1", "s2")
        .build()
    )

    results = flow.run(registry)
    assert results["s1"] == 7
    assert results["s2"] == 14


def test_process_executor_basic():
    # Use importable actions (koala.actions) and provide registry as dotted paths
    registry = {"add": "koala.actions.add", "mul": "koala.actions.mul"}

    flow = DAGFlow(id="p1")
    flow.add_step(Step(id="s1", action="add", args={"a": 5, "b": 7}))
    flow.add_step(Step(id="s2", action="mul", args={"x": "$result.s1", "y": 3}))
    flow.add_edge("s1", "s2")

    from koala.flow import ProcessExecutor

    pex = ProcessExecutor(max_workers=2)
    results = pex.run_dagflow(flow, registry)
    assert results["s1"] == 12
    assert results["s2"] == 36
