"""
Houdini Extraction Plugin

Data extraction plugin that registers handlers with the Houdini bridge server.
Implements the pixel_vision interface contract (§2-§3) for scene graph crawling,
geometry extraction, AOV discovery, and camera configuration reading.

Usage:
    Loaded automatically by the bridge server via soft-import.
    Falls back gracefully if not installed — existing 32 tools continue to work.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

CONTRACT_VERSION = "1.0"


class ExtractionPlugin:
    """Extraction plugin that registers query handlers with the bridge."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., dict[str, Any]]] = {}

    def register_handlers(self, handler_class: type) -> None:
        """Register extraction GET/POST handlers on the bridge handler class.

        Adds extraction handler functions to the bridge's dispatch table
        so they can be called via HTTP routes like /extract/geo_info.

        Args:
            handler_class: The HoudiniBridgeHandler class to register on.
        """
        from .aov import handle_aov_list
        from .camera import handle_camera_get
        from .geo import handle_attrib_read, handle_geo_info
        from .invalidation import handle_drain_events

        get_handlers: dict[str, Callable[..., dict[str, Any]]] = {
            '/extract/geo_info': handle_geo_info,
            '/extract/attrib_read': handle_attrib_read,
            '/extract/camera_get': handle_camera_get,
            '/extract/aov_list': handle_aov_list,
            '/extract/events': handle_drain_events,
        }

        # Store on handler class as class variable for dispatch
        if not hasattr(handler_class, '_extraction_handlers'):
            handler_class._extraction_handlers = {}  # type: ignore[attr-defined]
        handler_class._extraction_handlers.update(get_handlers)  # type: ignore[attr-defined]

        self._handlers = get_handlers
