# `koala.core`

L1 primitives — pure types, Protocols, and small dataclasses. Zero runtime
dependencies beyond stdlib and pydantic.

See also the [Runnable + events concept page](../concepts/runnable-and-events.md).

## Messages

::: koala.core.messages.Message
::: koala.core.messages.Role
::: koala.core.messages.TextBlock
::: koala.core.messages.ThinkingBlock
::: koala.core.messages.ImageBlock
::: koala.core.messages.ToolCallBlock
::: koala.core.messages.ToolResultBlock

## Events

::: koala.core.events.Start
::: koala.core.events.ModelDelta
::: koala.core.events.ThinkingDelta
::: koala.core.events.ModelMessage
::: koala.core.events.ToolCall
::: koala.core.events.ToolResult
::: koala.core.events.AwaitingApproval
::: koala.core.events.UsageEvent
::: koala.core.events.Output
::: koala.core.events.Error
::: koala.core.events.Done

## Runnable protocols

::: koala.core.runnable.Runnable
::: koala.core.runnable.Channel
::: koala.core.runnable.ainvoke
::: koala.core.runnable.invoke
::: koala.core.runnable.acollect

## Run context

::: koala.core.context.RunContext
::: koala.core.context.CancelToken

## Types

::: koala.core.types.Usage
::: koala.core.types.ModelRef
::: koala.core.types.parse_model_ref

## Capabilities

::: koala.core.capabilities.Capability

## Approval primitives

::: koala.core.approval.ApprovalRule
::: koala.core.approval.ApprovalResult
