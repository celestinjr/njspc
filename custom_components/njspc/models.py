"""Runtime models for the njsPC integration."""

from __future__ import annotations

from dataclasses import dataclass

from pynjspc import NjsPCClient

from .coordinator import NjspcCoordinator


@dataclass(slots=True)
class NjspcRuntimeData:
    """Entry-scoped runtime state."""

    client: NjsPCClient
    coordinator: NjspcCoordinator
