"""
Geometry extraction handlers.

Implements handle_geo_info (§2.3 Geometry Summary) and handle_attrib_read
(§3.1 bulk attribute reading) for the extraction plugin.
"""

from __future__ import annotations

import json
import struct
from typing import Any

from .file_ref import write_file_ref_pair
from .serializers import classify_attrib_type

# Houdini module — only available inside Houdini
try:
    import hou

    IN_HOUDINI = True
except ImportError:
    IN_HOUDINI = False
    hou = None

# Cap for prim type iteration to avoid perf issues on huge geo
_PRIM_TYPE_SCAN_CAP = 100_000

# Threshold for inline vs file_ref (1 MB)
_INLINE_THRESHOLD = 1_000_000


def handle_geo_info(params: dict[str, str]) -> dict[str, Any]:
    """Extract geometry summary matching §2.3 schema.

    Args:
        params: Query parameters with 'path' (node path).

    Returns:
        Geometry Summary dict per §2.3 contract.
    """
    path = params.get('path')
    if not path:
        return {'error': True, 'code': 'PARM_NOT_FOUND', 'message': "Missing 'path' parameter"}

    node = hou.node(path)
    if not node:
        return {'error': True, 'code': 'NODE_NOT_FOUND', 'message': f'No node exists at path {path}'}

    if not hasattr(node, 'geometry'):
        return {
            'error': True,
            'code': 'TYPE_MISMATCH',
            'message': f'Node {path} does not have geometry output',
        }

    geo = node.geometry()
    if geo is None:
        return {
            'error': True,
            'code': 'COOK_ERROR',
            'message': f'Node {path} has no cooked geometry',
        }

    # Counts via intrinsics
    point_count = geo.intrinsicValue('pointcount')
    prim_count = geo.intrinsicValue('primitivecount')
    vertex_count = geo.intrinsicValue('vertexcount')

    # Prim type breakdown (capped for perf)
    prim_types: dict[str, int] = {}
    scan_count = min(prim_count, _PRIM_TYPE_SCAN_CAP)
    if scan_count > 0:
        prims = geo.prims()
        for i in range(scan_count):
            ptype = prims[i].type().name()
            prim_types[ptype] = prim_types.get(ptype, 0) + 1
        if prim_count > _PRIM_TYPE_SCAN_CAP:
            prim_types['_truncated_at'] = _PRIM_TYPE_SCAN_CAP

    # Bounding box
    bbox = geo.boundingBox()
    bounds = {
        'min': list(bbox.minvec()),
        'max': list(bbox.maxvec()),
    }

    # Attribute catalog by class
    attributes: dict[str, dict[str, dict[str, Any]]] = {
        'point': {},
        'primitive': {},
        'vertex': {},
        'detail': {},
    }

    _class_map = {
        'Point': 'point',
        'Prim': 'primitive',
        'Vertex': 'vertex',
        'Global': 'detail',
    }

    for attrib in geo.pointAttribs():
        atype, asize = classify_attrib_type(attrib)
        attributes['point'][attrib.name()] = {'type': atype, 'size': asize}

    for attrib in geo.primAttribs():
        atype, asize = classify_attrib_type(attrib)
        attributes['primitive'][attrib.name()] = {'type': atype, 'size': asize}

    for attrib in geo.vertexAttribs():
        atype, asize = classify_attrib_type(attrib)
        attributes['vertex'][attrib.name()] = {'type': atype, 'size': asize}

    for attrib in geo.globalAttribs():
        atype, asize = classify_attrib_type(attrib)
        attributes['detail'][attrib.name()] = {'type': atype, 'size': asize}

    # Groups
    groups: dict[str, list[str]] = {
        'point': [g.name() for g in geo.pointGroups()],
        'prim': [g.name() for g in geo.primGroups()],
    }

    # Memory estimate
    memory_bytes = -1
    try:
        memory_bytes = geo.intrinsicValue('memoryusage')
    except Exception:
        pass

    return {
        'node_path': path,
        'point_count': point_count,
        'prim_count': prim_count,
        'vertex_count': vertex_count,
        'prim_types': prim_types,
        'bounds': bounds,
        'attributes': attributes,
        'groups': groups,
        'memory_bytes': memory_bytes,
    }


def handle_attrib_read(params: dict[str, str]) -> dict[str, Any]:
    """Bulk-read attribute values from geometry.

    Returns inline JSON for small data (<1MB) or a file_ref for larger data.
    Binary format is flat float32/int32 with a JSON metadata sidecar.

    Args:
        params: Query parameters:
            - path: node path
            - attrib_class: 'point', 'prim', 'vertex', or 'detail'
            - attrib_name: attribute name
            - start: optional start index (default 0)
            - count: optional count (-1 for all, default -1)

    Returns:
        Either inline data or a file_ref dict.
    """
    path = params.get('path')
    attrib_class = params.get('attrib_class', 'point')
    attrib_name = params.get('attrib_name')
    start = int(params.get('start', '0'))
    count = int(params.get('count', '-1'))

    if not path:
        return {'error': True, 'code': 'PARM_NOT_FOUND', 'message': "Missing 'path' parameter"}
    if not attrib_name:
        return {'error': True, 'code': 'PARM_NOT_FOUND', 'message': "Missing 'attrib_name' parameter"}

    node = hou.node(path)
    if not node:
        return {'error': True, 'code': 'NODE_NOT_FOUND', 'message': f'No node exists at path {path}'}

    if not hasattr(node, 'geometry'):
        return {
            'error': True,
            'code': 'TYPE_MISMATCH',
            'message': f'Node {path} does not have geometry output',
        }

    geo = node.geometry()
    if geo is None:
        return {
            'error': True,
            'code': 'COOK_ERROR',
            'message': f'Node {path} has no cooked geometry',
        }

    # Find the attribute
    attrib = _get_attrib(geo, attrib_class, attrib_name)
    if attrib is None:
        return {
            'error': True,
            'code': 'PARM_NOT_FOUND',
            'message': f'Attribute {attrib_name} not found in {attrib_class} class',
        }

    atype, asize = classify_attrib_type(attrib)
    data_type = attrib.dataType().name()  # 'Float', 'Int', 'String'

    # Read values using bulk API
    values = _read_bulk_attrib(geo, attrib_class, attrib_name, data_type, asize)
    if values is None:
        return {
            'error': True,
            'code': 'EXTRACTION_FAILED',
            'message': f'Failed to read attribute {attrib_name}',
        }

    # Apply start/count slicing
    total_elements = len(values) // asize if asize > 1 else len(values)
    if count == -1:
        count = total_elements - start
    end = min(start + count, total_elements)

    if asize > 1:
        values = values[start * asize : end * asize]
    else:
        values = values[start:end]

    # Decide inline vs file_ref
    estimated_bytes = len(values) * 4  # rough estimate (float32/int32)

    if estimated_bytes < _INLINE_THRESHOLD:
        # Return inline
        if asize > 1:
            # Reshape into tuples
            inline_values = [
                list(values[i : i + asize]) for i in range(0, len(values), asize)
            ]
        else:
            inline_values = list(values)

        return {
            'node_path': path,
            'attrib_class': attrib_class,
            'attrib_name': attrib_name,
            'type': atype,
            'size': asize,
            'count': end - start,
            'total': total_elements,
            'values': inline_values,
        }
    else:
        # Write binary file_ref
        if data_type == 'Float':
            fmt = f'<{len(values)}f'
        elif data_type == 'Int':
            fmt = f'<{len(values)}i'
        else:
            # String data goes as JSON
            json_bytes = json.dumps(list(values)).encode('utf-8')
            return write_file_ref_pair(
                binary_data=json_bytes,
                metadata={
                    'node_path': path,
                    'attrib_class': attrib_class,
                    'attrib_name': attrib_name,
                    'type': atype,
                    'size': asize,
                    'count': end - start,
                    'total': total_elements,
                    'encoding': 'json',
                },
                binary_ext='.json',
                prefix=f'attrib_{attrib_name}',
            )

        binary_data = struct.pack(fmt, *values)
        return write_file_ref_pair(
            binary_data=binary_data,
            metadata={
                'node_path': path,
                'attrib_class': attrib_class,
                'attrib_name': attrib_name,
                'type': atype,
                'size': asize,
                'count': end - start,
                'total': total_elements,
                'encoding': 'float32' if data_type == 'Float' else 'int32',
                'byte_order': 'little',
            },
            binary_ext='.bin',
            prefix=f'attrib_{attrib_name}',
        )


def _get_attrib(geo: Any, attrib_class: str, name: str) -> Any:
    """Look up an attribute by class and name."""
    if attrib_class == 'point':
        return geo.findPointAttrib(name)
    if attrib_class in ('prim', 'primitive'):
        return geo.findPrimAttrib(name)
    if attrib_class == 'vertex':
        return geo.findVertexAttrib(name)
    if attrib_class in ('detail', 'global'):
        return geo.findGlobalAttrib(name)
    return None


def _read_bulk_attrib(
    geo: Any,
    attrib_class: str,
    name: str,
    data_type: str,
    size: int,
) -> Any:
    """Read attribute values using Houdini's fast bulk API.

    Returns a flat tuple of values (for float/int) or list (for string).
    """
    try:
        if attrib_class == 'point':
            if data_type == 'Float':
                return geo.pointFloatAttribValues(name)
            if data_type == 'Int':
                return geo.pointIntAttribValues(name)
            if data_type == 'String':
                return geo.pointStringAttribValues(name)
        elif attrib_class in ('prim', 'primitive'):
            if data_type == 'Float':
                return geo.primFloatAttribValues(name)
            if data_type == 'Int':
                return geo.primIntAttribValues(name)
            if data_type == 'String':
                return geo.primStringAttribValues(name)
        elif attrib_class == 'vertex':
            if data_type == 'Float':
                return geo.vertexFloatAttribValues(name)
            if data_type == 'Int':
                return geo.vertexIntAttribValues(name)
            if data_type == 'String':
                return geo.vertexStringAttribValues(name)
        elif attrib_class in ('detail', 'global'):
            # Detail attribs don't have bulk API — read directly
            attrib = geo.findGlobalAttrib(name)
            if attrib is None:
                return None
            val = geo.attribValue(name)
            if isinstance(val, tuple):
                return val
            return (val,)
    except Exception:
        return None
    return None
