from koala.flow import FlowError, LocalExecutor, dag


def test_step_retries_and_succeeds():
    calls = {"n": 0}

    def flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    f = (
        dag("rflow")
        .step(
            "s",
            "flaky",
            __retry__={"max_attempts": 3, "backoff": 0.01, "multiplier": 1.0},
        )
        .build()
    )
    reg = {"flaky": flaky}
    ex = LocalExecutor(max_workers=1)
    res = ex.run_dagflow(f, reg)
    assert res["s"] == "ok"
    assert calls["n"] == 3


def test_step_retries_and_fails_after_exhaustion():
    calls = {"n": 0}

    def always_fail(**kwargs):
        calls["n"] += 1
        raise RuntimeError("permanent")

    f = (
        dag("rflow2")
        .step(
            "s",
            "bad",
            __retry__={"max_attempts": 2, "backoff": 0.01, "multiplier": 1.0},
        )
        .build()
    )
    reg = {"bad": always_fail}
    ex = LocalExecutor(max_workers=1)
    try:
        ex.run_dagflow(f, reg)
        raise AssertionError("expected FlowError")
    except FlowError:
        assert calls["n"] == 2
