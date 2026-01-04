"""Example Kafka consumer skeleton showing how to wire messages to the orchestrator.

This is intentionally dependency-free and demonstrates the wiring using a
simulated message source. Replace the simulated loop with a real Kafka client
implementation (aiokafka or confluent-kafka) for production.
"""

from __future__ import annotations

import time

from koala.flow import LocalExecutor, dag
from koala.orchestrator import Orchestrator
from koala.tools import default_registry


def simulated_consume_messages():
    # Simulate incoming messages that contain inline flows
    for i in range(3):
        flow = dag(f"kflow-{i}").step("a", "noop", x=i).build()
        yield {"type": "dag", "flow": flow.to_dict()}
        time.sleep(0.1)


def run_consumer():
    # placeholder adapter instance (not connected)
    # adapter = KafkaAdapter()  # placeholder; unused
    orch = Orchestrator()
    ex = LocalExecutor()

    for msg in simulated_consume_messages():
        # here you'd parse the Kafka message; we already have a payload dict
        payload = msg
        # naive: load flow and submit
        from koala.ingress import load_flow_from_payload

        flow = load_flow_from_payload(payload)
        run_id = orch.submit_flow(
            flow,
            ex,
            registry={
                name: meta.func for name, meta in default_registry._tools.items()
            },
        )
        print("Submitted run", run_id)


if __name__ == "__main__":
    run_consumer()
