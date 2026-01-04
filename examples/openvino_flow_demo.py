"""OpenVINO + Kola demo flow (optional).

Wires an OpenVINO inference function as a Kola tool and runs a tiny flow.
Requires `pip install openvino-dev` and a valid model path.
"""

from __future__ import annotations

import os
from typing import Any

from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool

try:
    import openvino as ov  # type: ignore
except Exception:
    ov = None


def _load_model(path: str):
    if ov is None:
        raise RuntimeError(
            "OpenVINO not installed. Install with `pip install openvino-dev`."
        )
    core = ov.Core()
    model = core.read_model(path)
    compiled = core.compile_model(model, "CPU")
    return compiled


# Singleton compiled model for demo purposes
_COMPILED_MODEL: Any = None


@tool("ov_infer")
def ov_infer(input_data: Any) -> Any:
    if ov is None:
        raise RuntimeError("OpenVINO not installed.")
    if _COMPILED_MODEL is None:
        raise RuntimeError("Model not loaded. Set KOLA_OV_MODEL path and run main().")
    return _COMPILED_MODEL([input_data])


def build_flow(input_data: Any):
    return dag("ov-demo").step("infer", "ov_infer", input_data=input_data).build()


def main():  # pragma: no cover
    model_path = os.environ.get("KOLA_OV_MODEL")
    if not model_path:
        print("Set env KOLA_OV_MODEL to a valid model path (XML/IR/ONNX).")
        return
    global _COMPILED_MODEL
    _COMPILED_MODEL = _load_model(model_path)
    flow = build_flow(input_data=[1.0, 2.0, 3.0])
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    ex = LocalExecutor()
    res = ex.run_dagflow(flow, registry)
    print("OpenVINO flow result:", res["infer"])


if __name__ == "__main__":  # pragma: no cover
    main()
