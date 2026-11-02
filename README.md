# njspc

Independent Home Assistant custom integration for nodejs-poolController.

This repository is being developed under the governed NJSPC-003 foundation
task. The foundation establishes config-entry setup, controller state
reconciliation, lifecycle handling, and deterministic tests. Equipment entity
platforms are intentionally out of scope for this slice.

## Development baseline

The integration currently depends on the local NJSPC-002 `pynjspc` checkout
during development. No public `pynjspc` release or remote `njspc` repository is
assumed by this foundation.

The custom integration lives under `custom_components/njspc`.
