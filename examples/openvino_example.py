"""Optional OpenVINO example stub.

This file demonstrates how a user could integrate OpenVINO for model
inference in agent tools. It is not imported by default and requires
`pip install openvino-dev`.
"""

from __future__ import annotations

from typing import Any

try:
    import openvino as ov  # type: ignore
except Exception:  # pragma: no cover
    ov = None


def load_model(path: str):
    if ov is None:
        raise RuntimeError(
            "OpenVINO is not installed. Install with `pip install openvino-dev`. "
        )
    core = ov.Core()
    model = core.read_model(path)
    compiled = core.compile_model(model, "CPU")
    return compiled


def infer(compiled_model: Any, input_data: Any) -> Any:
    if ov is None:
        raise RuntimeError("OpenVINO is not installed.")
    return compiled_model([input_data])


def main():  # pragma: no cover
    print("This is a stub. Install OpenVINO and provide a model path to use.")


if __name__ == "__main__":  # pragma: no cover
    main()
