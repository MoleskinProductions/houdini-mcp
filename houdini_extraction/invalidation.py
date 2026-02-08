"""
Scene invalidation event system.

Registers Houdini event callbacks to track scene changes and queues
invalidation events that the MCP server can poll via /extract/events.
Implements §5.2 of the pixel_vision interface contract.

Required event types (all 7):
  cook_complete, parm_changed, node_created, node_deleted,
  connection_changed, frame_changed, hip_saved
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

# Root network paths to monitor
_ROOT_PATHS = ('/obj', '/stage', '/tasks', '/out', '/shop', '/mat')


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

# Node event types we monitor on root networks.
# These propagate from children, so registering on /obj catches
# events from /obj/geo1/scatter1 etc.
_NODE_EVENT_TYPES: tuple[Any, ...] = ()


def start_invalidation() -> None:
    """Register Houdini event callbacks for scene change tracking.

    Called by the bridge on startup. Hooks:
    - hipFile events → hip_saved
    - Root network child events → node_created, node_deleted
    - Root network node events → parm_changed, cook_complete, connection_changed
    - Playbar events → frame_changed

    Safe to call multiple times (idempotent).
    """
    global _registered, _NODE_EVENT_TYPES

    if not IN_HOUDINI or _registered:
        return

    # Build the event type tuple (hou must be available)
    _NODE_EVENT_TYPES = (
        hou.nodeEventType.ChildCreated,
        hou.nodeEventType.ChildDeleted,
        hou.nodeEventType.ParmTupleChanged,
        hou.nodeEventType.InputRewired,
        hou.nodeEventType.AppearanceChanged,
    )

    # Hip file events (save, load)
    try:
        hou.hipFile.addEventCallback(_hip_event_callback)
    except Exception:
        pass

    # Node events on root networks
    for root_path in _ROOT_PATHS:
        try:
            root = hou.node(root_path)
            if root is not None:
                root.addEventCallback(_NODE_EVENT_TYPES, _node_event_callback)
        except Exception:
            continue

    # Frame changed via playbar
    try:
        hou.playbar.addEventCallback(_playbar_event_callback)
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

    for root_path in _ROOT_PATHS:
        try:
            root = hou.node(root_path)
            if root is not None:
                root.removeEventCallback(_NODE_EVENT_TYPES, _node_event_callback)
        except Exception:
            continue

    try:
        hou.playbar.removeEventCallback(_playbar_event_callback)
    except Exception:
        pass

    _registered = False


# ============================================================================
# Callback implementations
# ============================================================================

def _hip_event_callback(event_type: Any) -> None:
    """Callback for hipFile events → hip_saved."""
    try:
        event_name = str(event_type)
        if 'AfterSave' in event_name:
            _push_event('hip_saved', scope='scene', path=hou.hipFile.path())
        elif 'AfterLoad' in event_name or 'AfterClear' in event_name:
            _push_event('hip_saved', scope='scene', path=hou.hipFile.path())
    except Exception:
        pass


def _node_event_callback(event_type: Any, **kwargs: Any) -> None:
    """Callback for node events on root networks.

    Handles: ChildCreated, ChildDeleted, ParmTupleChanged, InputRewired,
    AppearanceChanged (→ cook_complete).
    """
    try:
        event_name = str(event_type)
        node = kwargs.get('node')
        node_path = node.path() if node else '/'

        if 'ChildCreated' in event_name:
            child_node = kwargs.get('child_node')
            child_path = child_node.path() if child_node else node_path
            _push_event('node_created', scope='network', path=child_path)
        elif 'ChildDeleted' in event_name:
            _push_event('node_deleted', scope='network', path=node_path)
        elif 'ParmTupleChanged' in event_name:
            parm_tuple = kwargs.get('parm_tuple')
            parm_name = parm_tuple.name() if parm_tuple else ''
            _push_event('parm_changed', scope='node', path=f'{node_path}/{parm_name}')
        elif 'InputRewired' in event_name:
            _push_event('connection_changed', scope='node', path=node_path)
        elif 'AppearanceChanged' in event_name:
            # AppearanceChanged fires when a node finishes cooking
            # (badge/color update). Used as proxy for cook_complete.
            _push_event('cook_complete', scope='node', path=node_path)
    except Exception:
        pass


def _playbar_event_callback(event_type: Any, frame: float) -> None:
    """Callback for playbar events → frame_changed."""
    try:
        event_name = str(event_type)
        if 'FrameChanged' in event_name:
            _push_event('frame_changed', scope='scene', path=f'frame:{frame}')
    except Exception:
        pass
