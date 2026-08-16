# `koala.tools`

L3 — tools layer. See the [Tools guide](../guide/tools.md),
[Approval + HITL guide](../guide/approval-hitl.md), and [MCP guide](../guide/mcp.md).

## Base types

::: koala.tools.base.BaseTool

## Function tools

::: koala.tools.function_tool.FunctionTool
::: koala.tools.function_tool.tool

## Schema derivation

::: koala.tools.schema.ToolSpec
::: koala.tools.schema.build_tool_spec
::: koala.tools.schema.parse_google_docstring

## Approval rules

::: koala.tools.approval.evaluate_approval_chain
::: koala.tools.approval.DenyList
::: koala.tools.approval.AllowList
::: koala.tools.approval.AlwaysAsk
::: koala.tools.approval.AlwaysAllow
::: koala.tools.approval.RequireApprovalFor

## MCP client

Requires the `[mcp]` extra.

::: koala.tools.mcp.MCPTool
::: koala.tools.mcp.MCPToolset
::: koala.tools.mcp.list_mcp_tools

## Errors

::: koala.tools.errors.ToolError
::: koala.tools.errors.ToolValidationError
::: koala.tools.errors.ToolExecutionError
::: koala.tools.errors.ToolNotFoundError
