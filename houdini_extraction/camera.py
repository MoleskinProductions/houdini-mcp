"""
Camera extraction handler.

Implements handle_camera_get matching the §2.5 Camera Configuration schema.
"""

from __future__ import annotations

from typing import Any

# Houdini module — only available inside Houdini
try:
    import hou

    IN_HOUDINI = True
except ImportError:
    IN_HOUDINI = False
    hou = None


def handle_camera_get(params: dict[str, str]) -> dict[str, Any]:
    """Extract camera configuration matching §2.5 schema.

    Args:
        params: Query parameters with 'path' (camera node path).

    Returns:
        Camera Configuration dict per §2.5 contract.
    """
    path = params.get('path')
    if not path:
        return {'error': True, 'code': 'PARM_NOT_FOUND', 'message': "Missing 'path' parameter"}

    node = hou.node(path)
    if not node:
        return {'error': True, 'code': 'NODE_NOT_FOUND', 'message': f'No node exists at path {path}'}

    # Verify it's a camera
    type_name = node.type().name()
    if type_name not in ('cam', 'camera', 'stereocam'):
        return {
            'error': True,
            'code': 'TYPE_MISMATCH',
            'message': f'Node {path} is type {type_name}, not a camera',
        }

    # Read camera parameters
    focal_length = _parm_eval(node, 'focal', 50.0)
    aperture = _parm_eval(node, 'aperture', 41.4214)
    near_clip = _parm_eval(node, 'near', 0.01)
    far_clip = _parm_eval(node, 'far', 10000.0)

    # Resolution — try resx/resy first, fall back to res
    resx = _parm_eval(node, 'resx', None)
    resy = _parm_eval(node, 'resy', None)
    if resx is None or resy is None:
        res_tuple = node.parmTuple('res')
        if res_tuple:
            res_vals = res_tuple.eval()
            resx = res_vals[0] if len(res_vals) > 0 else 1920
            resy = res_vals[1] if len(res_vals) > 1 else 1080
        else:
            resx = 1920
            resy = 1080

    resolution = [int(resx), int(resy)]

    # World transform matrix (4x4 row-major)
    world_matrix = _get_world_matrix(node)

    # Extract translate and rotate as convenience fields
    translate = _extract_translate(node)
    rotate = _extract_rotate(node)

    return {
        'path': path,
        'resolution': resolution,
        'focal_length': focal_length,
        'aperture': aperture,
        'near_clip': near_clip,
        'far_clip': far_clip,
        'transform': {
            'translate': translate,
            'rotate': rotate,
            'world_matrix': world_matrix,
        },
    }


def _parm_eval(node: Any, name: str, default: Any) -> Any:
    """Safely evaluate a parameter, returning default if missing."""
    parm = node.parm(name)
    if parm is not None:
        return parm.eval()
    return default


def _get_world_matrix(node: Any) -> list[list[float]]:
    """Get the 4x4 world transform matrix as row-major nested lists.

    Args:
        node: A hou.ObjNode with worldTransform().

    Returns:
        4x4 list of lists (row-major).
    """
    try:
        m = node.worldTransform()
        return [
            [m.at(row, col) for col in range(4)]
            for row in range(4)
        ]
    except Exception:
        return [
            [1, 0, 0, 0],
            [0, 1, 0, 0],
            [0, 0, 1, 0],
            [0, 0, 0, 1],
        ]


def _extract_translate(node: Any) -> list[float]:
    """Extract world-space translation from node."""
    try:
        m = node.worldTransform()
        return [m.at(3, 0), m.at(3, 1), m.at(3, 2)]
    except Exception:
        t = node.parmTuple('t')
        if t:
            return list(t.eval())
        return [0.0, 0.0, 0.0]


def _extract_rotate(node: Any) -> list[float]:
    """Extract rotation (Euler degrees) from node's local parms."""
    r = node.parmTuple('r')
    if r:
        return list(r.eval())
    return [0.0, 0.0, 0.0]
