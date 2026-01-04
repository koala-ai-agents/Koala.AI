from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    data_path = root / "data" / "demo_input.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Demo data not found: {data_path}")

    # Agent A demo
    from importlib.util import module_from_spec, spec_from_file_location

    agent_a_path = root / "agent_a.py"
    spec = spec_from_file_location("agent_a", agent_a_path)
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore

    print("Running Agent A demo...")
    result_a = mod.run_demo(data_path)
    print("Agent A result:")
    print(result_a)

    # Agent B demo
    agent_b_path = root / "agent_b.py"
    spec_b = spec_from_file_location("agent_b", agent_b_path)
    assert spec_b and spec_b.loader
    mod_b = module_from_spec(spec_b)
    spec_b.loader.exec_module(mod_b)  # type: ignore

    print("\nRunning Agent B demo...")
    result_b = mod_b.run_demo(data_path)
    print("Agent B result:")
    print(result_b)


if __name__ == "__main__":
    main()
