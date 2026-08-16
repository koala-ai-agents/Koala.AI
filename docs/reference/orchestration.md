# `koala.orchestration`

L7 — DAG orchestration over any `Runnable`. See the [Flow guide](../guide/flow.md).

## Fluent builder

::: koala.orchestration.flow.flow
::: koala.orchestration.flow.FlowBuilder
::: koala.orchestration.flow.Step
::: koala.orchestration.flow.StepAction

## Flow

`Flow` is the DAG of steps returned by the builder. It also exposes two
one-line convenience runners that dispatch to the executors — useful for
one-shot scripts that don't need executor configuration.

::: koala.orchestration.flow.Flow

## Local executor

::: koala.orchestration.executor.LocalExecutor

## Errors

::: koala.orchestration.errors.FlowError
::: koala.orchestration.errors.StepExecutionError
