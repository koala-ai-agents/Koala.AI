# `koala.ui`

Terminal renderer. See the [Streaming guide](../guide/streaming.md).

## Public entry points

::: koala.ui.show.show
::: koala.ui.show.ashow

## Internal renderer

The state-machine event renderer used by both `show` and `ashow`. Not part
of the stable public API, but documented for callers who need custom
UIs that reuse the state-machine logic (session `ashow`, etc.).

::: koala.ui.show._render_events
::: koala.ui.show._render_event
