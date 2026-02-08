"""
Scene invalidation event system.

Registers Houdini event callbacks to track scene changes and queues
invalidation events that the MCP server can poll via /extract/events.
Implements §5.2 of the pixel_vision interface contract.
"""

from __future__ import annotations

import threading
import time
from typing import Any

# Houdini module — only available inside Houdini
try:
    import hou

    IN_HOUDINI = True
except ImportError:
    IN_HOUDINI = False
    hou = None

# Thread-safe event queue
_event_lock = threading.Lock()
_event_queue: list[dict[str, Any]] = []

# Maximum events to buffer before dropping oldest
_MAX_QUEUE_SIZE = 1000


def _push_event(
    event_type: str,
    scope: str = 'node',
    path: str = '/',
) -> None:
    """Push an invalidation event to the queue.

    Args:
        event_type: One of the §5.2 event types.
        scope: 'node', 'network', or 'scene'.
        path: Affected node/network path.
    """
    event = {
        'event': 'invalidate',
        'scope': scope,
        'path': path,
        'event_type': event_type,
        'timestamp': time.time(),
    }

    with _event_lock:
        _event_queue.append(event)
        # Trim oldest if over capacity
        if len(_event_queue) > _MAX_QUEUE_SIZE:
            del _event_queue[: len(_event_queue) - _MAX_QUEUE_SIZE]


def drain_events() -> list[dict[str, Any]]:
    """Drain all queued invalidation events.

    Returns:
        List of invalidation event dicts. Queue is cleared.
    """
    with _event_lock:
        events = list(_event_queue)
        _event_queue.clear()
    return events


def handle_drain_events(params: dict[str, str]) -> dict[str, Any]:
    """Bridge handler for /extract/events polling endpoint.

    Args:
        params: Query parameters (unused).

    Returns:
        Dict with 'events' list and 'count'.
    """
    events = drain_events()
    return {
        'events': events,
        'count': len(events),
    }


# ============================================================================
# Houdini event callback registration
# ============================================================================

_registered = False


def start_invalidation() -> None:
    """Register Houdini event callbacks for scene change tracking.

    Called by the bridge on startup. Hooks hipFile events and
    root network child events to detect scene changes.
    Safe to call multiple times (idempotent).
    """
    global _registered

    if not IN_HOUDINI or _registered:
        return

    try:
        # Hip file events (save, load)
        hou.hipFile.addEventCallback(_hip_event_callback)
    except Exception:
        pass

    try:
        # Child created/deleted on root networks
        for root_path in ('/obj', '/stage', '/tasks', '/out', '/shop', '/mat'):
            root = hou.node(root_path)
            if root is not None:
                root.addEventCallback(
                    (hou.nodeEventType.ChildCreated, hou.nodeEventType.ChildDeleted),
                    _child_event_callback,
                )
    except Exception:
        pass

    _registered = True


def stop_invalidation() -> None:
    """Unregister Houdini event callbacks.

    Called on bridge shutdown. Removes all registered callbacks.
    """
    global _registered

    if not IN_HOUDINI or not _registered:
        return

    try:
        hou.hipFile.removeEventCallback(_hip_event_callback)
    except Exception:
        pass

    try:
        for root_path in ('/obj', '/stage', '/tasks', '/out', '/shop', '/mat'):
            root = hou.node(root_path)
            if root is not None:
                root.removeEventCallback(
                    (hou.nodeEventType.ChildCreated, hou.nodeEventType.ChildDeleted),
                    _child_event_callback,
                )
    except Exception:
        pass

    _registered = False


def _hip_event_callback(event_type: Any) -> None:
    """Callback for hipFile events."""
    try:
        event_name = str(event_type)
        # Map hou.hipFileEventType to our event types
        if 'AfterSave' in event_name:
            _push_event('hip_saved', scope='scene', path=hou.hipFile.path())
        elif 'AfterLoad' in event_name or 'AfterClear' in event_name:
            _push_event('hip_saved', scope='scene', path=hou.hipFile.path())
    except Exception:
        pass


def _child_event_callback(event_type: Any, **kwargs: Any) -> None:
    """Callback for child created/deleted events on root networks."""
    try:
        child_node = kwargs.get('child_node')
        parent = kwargs.get('node')

        event_name = str(event_type)
        parent_path = parent.path() if parent else '/'
        child_path = child_node.path() if child_node else parent_path

        if 'ChildCreated' in event_name:
            _push_event('node_created', scope='network', path=child_path)
        elif 'ChildDeleted' in event_name:
            _push_event('node_deleted', scope='network', path=parent_path)
    except Exception:
        pass
