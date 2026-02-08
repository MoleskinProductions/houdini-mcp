"""
AOV extraction handler.

Implements handle_aov_list matching the §2.4 AOV Configuration schema.
Strategy: try USD UsdRender.Var prims on the stage first, then fall back
to Karma ROP parameter extraction.
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

# Canonical AOV set from §2.4
_CANONICAL_AOVS: dict[str, dict[str, str]] = {
    'depth': {'aov_type': 'DEPTH', 'data_type': 'float', 'format': 'float32', 'source': 'builtin'},
    'N_world': {'aov_type': 'NORMAL_WORLD', 'data_type': 'vector3f', 'format': 'float16', 'source': 'primvar'},
    'P_world': {'aov_type': 'POSITION_WORLD', 'data_type': 'point3f', 'format': 'float32', 'source': 'primvar'},
    'albedo': {'aov_type': 'ALBEDO', 'data_type': 'color3f', 'format': 'float16', 'source': 'lpe'},
    'diffuse': {'aov_type': 'DIFFUSE', 'data_type': 'color3f', 'format': 'float16', 'source': 'lpe'},
    'specular': {'aov_type': 'SPECULAR', 'data_type': 'color3f', 'format': 'float16', 'source': 'lpe'},
    'shadow': {'aov_type': 'SHADOW', 'data_type': 'float', 'format': 'float16', 'source': 'lpe'},
    'irradiance': {'aov_type': 'IRRADIANCE', 'data_type': 'color3f', 'format': 'float32', 'source': 'lpe'},
    'motion': {'aov_type': 'MOTION_VECTOR', 'data_type': 'vector3f', 'format': 'float16', 'source': 'builtin'},
    'crypto_object': {'aov_type': 'CRYPTOMATTE_OBJECT', 'data_type': 'color4f', 'format': 'float32', 'source': 'cryptomatte'},
    'crypto_material': {'aov_type': 'CRYPTOMATTE_MATERIAL', 'data_type': 'color4f', 'format': 'float32', 'source': 'cryptomatte'},
}


def handle_aov_list(params: dict[str, str]) -> dict[str, Any]:
    """Extract AOV configurations matching §2.4 schema.

    Strategy:
    1. If the node is a LOP node, try to read UsdRender.Var prims from the stage.
    2. Fall back to scanning Karma ROP parameters for AOV definitions.
    3. Map discovered AOVs to canonical names where possible.

    Args:
        params: Query parameters with 'path' (LOP/ROP node path).

    Returns:
        Dict with 'aovs' list of AOV Configuration dicts per §2.4.
    """
    path = params.get('path')
    if not path:
        return {'error': True, 'code': 'PARM_NOT_FOUND', 'message': "Missing 'path' parameter"}

    node = hou.node(path)
    if not node:
        return {'error': True, 'code': 'NODE_NOT_FOUND', 'message': f'No node exists at path {path}'}

    # Try USD stage first
    aovs = _try_usd_render_vars(node)

    # Fall back to Karma ROP parm scanning
    if not aovs:
        aovs = _try_karma_rop_parms(node)

    return {
        'path': path,
        'aovs': aovs,
        'count': len(aovs),
    }


def _try_usd_render_vars(node: Any) -> list[dict[str, Any]]:
    """Try to extract AOVs from USD RenderVar prims on the stage.

    Args:
        node: A hou.LopNode that may have a USD stage.

    Returns:
        List of AOV configuration dicts, empty if not applicable.
    """
    try:
        stage = node.stage()
    except AttributeError:
        return []

    if stage is None:
        return []

    aovs: list[dict[str, Any]] = []

    try:
        from pxr import UsdRender

        for prim in stage.Traverse():
            if not prim.IsA(UsdRender.Var):
                continue

            prim_name = prim.GetName()

            # Get data type from the prim
            data_type_attr = prim.GetAttribute('dataType')
            data_type = str(data_type_attr.Get()) if data_type_attr and data_type_attr.Get() else 'color3f'

            # Get source type
            source_name_attr = prim.GetAttribute('sourceName')
            source_name = str(source_name_attr.Get()) if source_name_attr and source_name_attr.Get() else prim_name

            source_type_attr = prim.GetAttribute('sourceType')
            source = str(source_type_attr.Get()) if source_type_attr and source_type_attr.Get() else 'raw'

            aov = _build_aov_entry(prim_name, data_type, source, source_name)
            aovs.append(aov)
    except ImportError:
        return []
    except Exception:
        return []

    return aovs


def _try_karma_rop_parms(node: Any) -> list[dict[str, Any]]:
    """Try to extract AOVs from Karma ROP parameters.

    Scans for image planes / extra image planes in the ROP parameters.

    Args:
        node: A hou.RopNode or similar with Karma render settings.

    Returns:
        List of AOV configuration dicts, empty if not applicable.
    """
    aovs: list[dict[str, Any]] = []

    # Check for karma-style extra image planes multiparm
    num_parm = node.parm('ar_aov_separate_file')
    if num_parm is None:
        num_parm = node.parm('vm_numaux')
    if num_parm is None:
        # Try direct extra planes count
        num_parm = node.parm('ar_numaux')

    if num_parm is not None:
        try:
            count = int(num_parm.eval())
            for i in range(1, count + 1):
                name_parm = node.parm(f'ar_aov_label{i}') or node.parm(f'vm_variable_plane{i}')
                if name_parm:
                    aov_name = name_parm.eval()
                    if aov_name:
                        aov = _build_aov_entry(aov_name)
                        aovs.append(aov)
        except Exception:
            pass

    # Also check for standard beauty / primary output
    if not aovs:
        # Even if we found no extra planes, check if this is a karma/mantra node
        type_name = node.type().name().lower()
        if 'karma' in type_name or 'mantra' in type_name or 'usdrender' in type_name:
            # Default: at minimum there's a beauty pass
            aovs.append({
                'aov_name': 'beauty',
                'aov_type': 'BEAUTY',
                'data_type': 'color3f',
                'format': 'float16',
                'source': 'builtin',
                'lpe': None,
                'normalize': False,
                'normalize_range': None,
            })

    return aovs


def _build_aov_entry(
    name: str,
    data_type: str | None = None,
    source: str | None = None,
    source_name: str | None = None,
) -> dict[str, Any]:
    """Build an AOV configuration dict, matching to canonical set when possible.

    Args:
        name: The AOV name.
        data_type: USD data type string (e.g., 'color3f').
        source: Source type string (e.g., 'lpe', 'primvar').
        source_name: Source name/LPE expression.

    Returns:
        AOV configuration dict per §2.4.
    """
    # Check canonical set
    canonical = _CANONICAL_AOVS.get(name)
    if canonical:
        return {
            'aov_name': name,
            'aov_type': canonical['aov_type'],
            'data_type': data_type or canonical['data_type'],
            'format': canonical['format'],
            'source': source or canonical['source'],
            'lpe': source_name if source in ('lpe',) else None,
            'normalize': name in ('depth',),
            'normalize_range': [0.0, 100.0] if name == 'depth' else None,
        }

    # Non-canonical: prefix with custom_ per contract
    aov_name = name if name.startswith('custom_') else f'custom_{name}'

    return {
        'aov_name': aov_name,
        'aov_type': name.upper(),
        'data_type': data_type or 'color3f',
        'format': 'float16',
        'source': source or 'raw',
        'lpe': source_name if source in ('lpe',) else None,
        'normalize': False,
        'normalize_range': None,
    }
