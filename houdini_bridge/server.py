"""
Houdini MCP Bridge Server

This module runs INSIDE Houdini (via shelf tool or 456.py) and exposes
a lightweight HTTP API for the external MCP server to communicate with.

Usage:
    # From Houdini Python Shell or shelf tool:
    from houdini_bridge.server import start_bridge
    start_bridge(port=8765)

    # Or add to 456.py for auto-start:
    import hou
    if hou.isUIAvailable():
        from houdini_bridge.server import start_bridge
        start_bridge()

Requirements:
    - Houdini 21+ (Python 3.11)
    - No external dependencies (stdlib only)
"""

from __future__ import annotations

import json
import threading
import traceback
from functools import wraps
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

# Houdini module - only available when running inside Houdini
try:
    import hou
    import hdefereval
    IN_HOUDINI = True
except ImportError:
    IN_HOUDINI = False
    hou = None
    hdefereval = None


def require_main_thread(func: Callable) -> Callable:
    """Decorator to ensure hou.* calls run on Houdini's main thread."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if IN_HOUDINI and hou.isUIAvailable():
            return hdefereval.executeInMainThreadWithResult(lambda: func(*args, **kwargs))
        return func(*args, **kwargs)
    return wrapper


class HoudiniBridgeHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for the Houdini bridge.
    
    All hou.* calls are executed on Houdini's main thread via hdefereval
    to avoid threading issues with Houdini's single-threaded UI.
    """
    
    # Timeout for Houdini operations (seconds)
    TIMEOUT = 30.0
    
    def do_GET(self):
        """Handle GET requests (read-only operations)."""
        parsed = urlparse(self.path)
        route = parsed.path
        params = {k: v[0] if len(v) == 1 else v 
                  for k, v in parse_qs(parsed.query).items()}
        
        routes = {
            '/ping': self.handle_ping,
            '/scene/info': self.handle_scene_info,
            '/node/get': self.handle_node_get,
            '/node/tree': self.handle_node_tree,
            '/node/search': self.handle_node_search,
            '/parm/get': self.handle_parm_get,
            '/parm/template': self.handle_parm_template,
            '/cook/status': self.handle_cook_status,
            '/hda/list': self.handle_hda_list,
        }
        
        handler = routes.get(route)
        if handler:
            try:
                handler(params)
            except Exception as e:
                self.send_error_json(500, f"Internal error: {str(e)}\n{traceback.format_exc()}")
        else:
            self.send_error_json(404, f"Unknown route: {route}")
    
    def do_POST(self):
        """Handle POST requests (mutations)."""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length)) if content_length > 0 else {}
        except json.JSONDecodeError as e:
            self.send_error_json(400, f"Invalid JSON: {str(e)}")
            return
        
        parsed = urlparse(self.path)
        route = parsed.path
        
        routes = {
            '/node/create': self.handle_node_create,
            '/node/delete': self.handle_node_delete,
            '/node/rename': self.handle_node_rename,
            '/node/connect': self.handle_node_connect,
            '/node/disconnect': self.handle_node_disconnect,
            '/node/flag': self.handle_node_flag,
            '/node/layout': self.handle_node_layout,
            '/parm/set': self.handle_parm_set,
            '/parm/revert': self.handle_parm_revert,
            '/parm/expression': self.handle_parm_expression,
            '/scene/save': self.handle_scene_save,
            '/frame/set': self.handle_frame_set,
            '/geo/export': self.handle_geo_export,
            '/render/snapshot': self.handle_render_snapshot,
            '/render/flipbook': self.handle_render_flipbook,
            '/batch': self.handle_batch,
        }
        
        handler = routes.get(route)
        if handler:
            try:
                handler(body)
            except Exception as e:
                self.send_error_json(500, f"Internal error: {str(e)}\n{traceback.format_exc()}")
        else:
            self.send_error_json(404, f"Unknown route: {route}")
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    # =========================================================================
    # GET Handlers (Read Operations)
    # =========================================================================
    
    def handle_ping(self, params: dict):
        """Health check and version info."""
        @require_main_thread
        def get_info():
            return {
                'status': 'ok',
                'houdini_version': hou.applicationVersionString(),
                'hip_file': hou.hipFile.path(),
                'hip_name': hou.hipFile.basename(),
                'license': hou.licenseCategory().name(),
                'is_apprentice': hou.isApprentice(),
            }
        
        self.send_json(get_info())
    
    def handle_scene_info(self, params: dict):
        """Get comprehensive scene information."""
        @require_main_thread
        def get_info():
            hip = hou.hipFile
            
            # Count nodes by context
            contexts = {}
            for ctx in ['/obj', '/shop', '/mat', '/stage', '/tasks', '/ch', '/out']:
                node = hou.node(ctx)
                contexts[ctx.lstrip('/')] = len(node.children()) if node else 0
            
            return {
                'hip_file': hip.path(),
                'hip_name': hip.basename(),
                'has_unsaved_changes': hip.hasUnsavedChanges(),
                'fps': hou.fps(),
                'frame_range': list(hou.playbar.frameRange()),
                'current_frame': hou.frame(),
                'time': hou.time(),
                'contexts': contexts,
                'memory_usage_mb': round(hou.memoryUsage() / (1024 * 1024), 2),
            }
        
        self.send_json(get_info())
    
    def handle_node_get(self, params: dict):
        """Get detailed info about a specific node."""
        path = params.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def get_node():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            return self._serialize_node(node, include_parms=True)
        
        self.send_json(get_node())
    
    def handle_node_tree(self, params: dict):
        """Get hierarchical node tree from a root."""
        root = params.get('root', '/obj')
        depth = int(params.get('depth', 2))
        
        @require_main_thread
        def get_tree():
            root_node = hou.node(root)
            if not root_node:
                return {'error': f'Root node not found: {root}'}
            
            def traverse(node, current_depth):
                data = {
                    'path': node.path(),
                    'name': node.name(),
                    'type': node.type().name(),
                    'type_label': node.type().description(),
                }
                
                if current_depth < depth and hasattr(node, 'children'):
                    children = node.children()
                    if children:
                        data['children'] = [
                            traverse(child, current_depth + 1) 
                            for child in children
                        ]
                
                return data
            
            return traverse(root_node, 0)
        
        self.send_json(get_tree())
    
    def handle_node_search(self, params: dict):
        """Search for nodes by name or type."""
        pattern = params.get('pattern', '*')
        node_type = params.get('type')
        root = params.get('root', '/')
        
        @require_main_thread
        def search():
            root_node = hou.node(root)
            if not root_node:
                return {'error': f'Root not found: {root}'}
            
            results = []
            for node in root_node.allSubChildren():
                name_match = pattern == '*' or pattern.lower() in node.name().lower()
                type_match = not node_type or node.type().name() == node_type
                
                if name_match and type_match:
                    results.append({
                        'path': node.path(),
                        'name': node.name(),
                        'type': node.type().name(),
                    })
            
            return {'results': results, 'count': len(results)}
        
        self.send_json(search())
    
    def handle_parm_get(self, params: dict):
        """Get parameter value(s) from a node."""
        path = params.get('path')
        parm_name = params.get('parm')
        
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def get_parms():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            if parm_name:
                # Single parameter
                parm = node.parm(parm_name)
                if parm:
                    return self._serialize_parm(parm)
                
                parm_tuple = node.parmTuple(parm_name)
                if parm_tuple:
                    return self._serialize_parm_tuple(parm_tuple)
                
                return {'error': f'Parameter not found: {parm_name}'}
            else:
                # All parameters (non-default only for brevity)
                parms = []
                for parm in node.parms():
                    if not parm.isAtDefault():
                        parms.append(self._serialize_parm(parm))
                
                return {
                    'node': path,
                    'parm_count': len(node.parms()),
                    'modified_parms': parms,
                }
        
        self.send_json(get_parms())
    
    def handle_parm_template(self, params: dict):
        """Get full parameter template (schema) for a node."""
        path = params.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def get_template():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            def serialize_template(tmpl):
                data = {
                    'name': tmpl.name(),
                    'label': tmpl.label(),
                    'type': tmpl.type().name(),
                }
                
                if hasattr(tmpl, 'defaultValue'):
                    data['default'] = tmpl.defaultValue()
                if hasattr(tmpl, 'minValue'):
                    data['min'] = tmpl.minValue()
                if hasattr(tmpl, 'maxValue'):
                    data['max'] = tmpl.maxValue()
                if hasattr(tmpl, 'menuItems'):
                    data['menu'] = list(tmpl.menuItems())
                
                return data
            
            templates = []
            for tmpl in node.parmTemplateGroup().entries():
                templates.append(serialize_template(tmpl))
            
            return {'node': path, 'templates': templates}
        
        self.send_json(get_template())
    
    def handle_cook_status(self, params: dict):
        """Get current cook/simulation status."""
        @require_main_thread
        def get_status():
            return {
                'is_cooking': hou.isSimulating(),
                'current_frame': hou.frame(),
                'current_time': hou.time(),
                'memory_usage_mb': round(hou.memoryUsage() / (1024 * 1024), 2),
                'cache_memory_mb': round(hou.cacheMemoryUsage() / (1024 * 1024), 2),
            }
        
        self.send_json(get_status())
    
    def handle_hda_list(self, params: dict):
        """List available HDA definitions."""
        category = params.get('category')
        
        @require_main_thread
        def list_hdas():
            hdas = []
            for definition in hou.hda.loadedFiles():
                for node_type in hou.hda.definitionsInFile(definition):
                    if category and node_type.category().name() != category:
                        continue
                    hdas.append({
                        'name': node_type.name(),
                        'label': node_type.description(),
                        'category': node_type.category().name(),
                        'file': definition,
                    })
            return {'hdas': hdas, 'count': len(hdas)}
        
        self.send_json(list_hdas())
    
    # =========================================================================
    # POST Handlers (Mutations)
    # =========================================================================
    
    def handle_node_create(self, body: dict):
        """Create a new node."""
        parent_path = body.get('parent', '/obj')
        node_type = body.get('type')
        name = body.get('name')
        position = body.get('position')
        
        if not node_type:
            self.send_error_json(400, "Missing 'type' parameter")
            return
        
        @require_main_thread
        def create():
            parent = hou.node(parent_path)
            if not parent:
                return {'error': f'Parent not found: {parent_path}'}
            
            try:
                with hou.undos.group("MCP: Create Node"):
                    node = parent.createNode(node_type, node_name=name)
                    if position:
                        node.setPosition(hou.Vector2(position))
                    else:
                        parent.layoutChildren()
                    
                    return {
                        'success': True,
                        'path': node.path(),
                        'name': node.name(),
                        'type': node.type().name(),
                    }
            except hou.OperationFailed as e:
                return {'error': str(e)}
        
        self.send_json(create())
    
    def handle_node_delete(self, body: dict):
        """Delete a node."""
        path = body.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def delete():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            try:
                with hou.undos.group("MCP: Delete Node"):
                    node.destroy()
                    return {'success': True, 'deleted': path}
            except hou.OperationFailed as e:
                return {'error': str(e)}
        
        self.send_json(delete())
    
    def handle_node_rename(self, body: dict):
        """Rename a node."""
        path = body.get('path')
        new_name = body.get('name')
        
        if not path or not new_name:
            self.send_error_json(400, "Missing 'path' or 'name' parameter")
            return
        
        @require_main_thread
        def rename():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            try:
                with hou.undos.group("MCP: Rename Node"):
                    node.setName(new_name)
                    return {
                        'success': True,
                        'old_path': path,
                        'new_path': node.path(),
                        'name': node.name(),
                    }
            except hou.OperationFailed as e:
                return {'error': str(e)}
        
        self.send_json(rename())
    
    def handle_node_connect(self, body: dict):
        """Connect two nodes."""
        from_path = body.get('from')
        to_path = body.get('to')
        from_output = body.get('from_output', 0)
        to_input = body.get('to_input', 0)
        
        if not from_path or not to_path:
            self.send_error_json(400, "Missing 'from' or 'to' parameter")
            return
        
        @require_main_thread
        def connect():
            from_node = hou.node(from_path)
            to_node = hou.node(to_path)
            
            if not from_node:
                return {'error': f'Source node not found: {from_path}'}
            if not to_node:
                return {'error': f'Destination node not found: {to_path}'}
            
            try:
                with hou.undos.group("MCP: Connect Nodes"):
                    to_node.setInput(to_input, from_node, from_output)
                    return {
                        'success': True,
                        'from': from_path,
                        'to': to_path,
                        'from_output': from_output,
                        'to_input': to_input,
                    }
            except hou.OperationFailed as e:
                return {'error': str(e)}
        
        self.send_json(connect())
    
    def handle_node_disconnect(self, body: dict):
        """Disconnect a node input."""
        path = body.get('path')
        input_index = body.get('input', 0)
        
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def disconnect():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            try:
                with hou.undos.group("MCP: Disconnect Node"):
                    node.setInput(input_index, None)
                    return {'success': True, 'path': path, 'input': input_index}
            except hou.OperationFailed as e:
                return {'error': str(e)}
        
        self.send_json(disconnect())
    
    def handle_node_flag(self, body: dict):
        """Set node flags (display, render, bypass, etc.)."""
        path = body.get('path')
        flag = body.get('flag')
        value = body.get('value', True)
        
        if not path or not flag:
            self.send_error_json(400, "Missing 'path' or 'flag' parameter")
            return
        
        @require_main_thread
        def set_flag():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            try:
                with hou.undos.group("MCP: Set Flag"):
                    if flag == 'display':
                        node.setDisplayFlag(value)
                    elif flag == 'render':
                        node.setRenderFlag(value)
                    elif flag == 'bypass':
                        node.bypass(value)
                    elif flag == 'template':
                        node.setTemplateFlag(value)
                    elif flag == 'selectable':
                        node.setSelectableInViewport(value)
                    else:
                        return {'error': f'Unknown flag: {flag}'}
                    
                    return {'success': True, 'path': path, 'flag': flag, 'value': value}
            except (hou.OperationFailed, AttributeError) as e:
                return {'error': str(e)}
        
        self.send_json(set_flag())
    
    def handle_node_layout(self, body: dict):
        """Auto-layout children of a node."""
        path = body.get('path', '/obj')
        
        @require_main_thread
        def layout():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            try:
                node.layoutChildren()
                return {'success': True, 'path': path}
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(layout())
    
    def handle_parm_set(self, body: dict):
        """Set parameter value."""
        path = body.get('path')
        parm_name = body.get('parm')
        value = body.get('value')
        
        if not path or not parm_name or value is None:
            self.send_error_json(400, "Missing 'path', 'parm', or 'value' parameter")
            return
        
        @require_main_thread
        def set_parm():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            parm = node.parm(parm_name)
            parm_tuple = node.parmTuple(parm_name) if not parm else None
            
            if not parm and not parm_tuple:
                return {'error': f'Parameter not found: {parm_name}'}
            
            try:
                with hou.undos.group("MCP: Set Parameter"):
                    if parm:
                        parm.set(value)
                        return {
                            'success': True,
                            'parm': parm_name,
                            'value': parm.eval(),
                        }
                    else:
                        if isinstance(value, (list, tuple)):
                            parm_tuple.set(value)
                        else:
                            return {'error': f'{parm_name} is a tuple, expected list/tuple'}
                        return {
                            'success': True,
                            'parm': parm_name,
                            'value': list(parm_tuple.eval()),
                        }
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(set_parm())
    
    def handle_parm_revert(self, body: dict):
        """Revert parameter to default value."""
        path = body.get('path')
        parm_name = body.get('parm')
        
        if not path or not parm_name:
            self.send_error_json(400, "Missing 'path' or 'parm' parameter")
            return
        
        @require_main_thread
        def revert():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            parm = node.parm(parm_name)
            if not parm:
                return {'error': f'Parameter not found: {parm_name}'}
            
            try:
                with hou.undos.group("MCP: Revert Parameter"):
                    parm.revertToDefaults()
                    return {
                        'success': True,
                        'parm': parm_name,
                        'value': parm.eval(),
                    }
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(revert())
    
    def handle_parm_expression(self, body: dict):
        """Set parameter expression."""
        path = body.get('path')
        parm_name = body.get('parm')
        expression = body.get('expression')
        language = body.get('language', 'hscript')
        
        if not all([path, parm_name, expression]):
            self.send_error_json(400, "Missing required parameters")
            return
        
        @require_main_thread
        def set_expr():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            parm = node.parm(parm_name)
            if not parm:
                return {'error': f'Parameter not found: {parm_name}'}
            
            try:
                with hou.undos.group("MCP: Set Expression"):
                    lang = (hou.exprLanguage.Hscript if language == 'hscript' 
                            else hou.exprLanguage.Python)
                    parm.setExpression(expression, lang)
                    return {
                        'success': True,
                        'parm': parm_name,
                        'expression': expression,
                        'language': language,
                        'value': parm.eval(),
                    }
            except Exception as e:
                return {'error': f'Invalid expression: {str(e)}'}
        
        self.send_json(set_expr())
    
    def handle_scene_save(self, body: dict):
        """Save the current scene."""
        path = body.get('path')  # Optional, saves to current if not specified
        
        @require_main_thread
        def save():
            try:
                if path:
                    hou.hipFile.save(path)
                else:
                    hou.hipFile.save()
                return {
                    'success': True,
                    'path': hou.hipFile.path(),
                }
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(save())
    
    def handle_frame_set(self, body: dict):
        """Set current frame."""
        frame = body.get('frame')
        if frame is None:
            self.send_error_json(400, "Missing 'frame' parameter")
            return
        
        @require_main_thread
        def set_frame():
            try:
                hou.setFrame(frame)
                return {'success': True, 'frame': hou.frame()}
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(set_frame())
    
    def handle_geo_export(self, body: dict):
        """Export geometry from a SOP node."""
        path = body.get('path')
        format = body.get('format', 'obj')
        output = body.get('output')
        
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return
        
        @require_main_thread
        def export():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}
            
            if not hasattr(node, 'geometry'):
                return {'error': 'Node does not have geometry output'}
            
            geo = node.geometry()
            if geo is None:
                return {'error': 'Node has no cooked geometry'}
            
            # Generate output path
            if not output:
                import tempfile
                import os
                ext_map = {
                    'obj': '.obj',
                    'gltf': '.gltf',
                    'glb': '.glb',
                    'usd': '.usd',
                    'usda': '.usda',
                    'ply': '.ply',
                    'bgeo': '.bgeo.sc',
                }
                ext = ext_map.get(format, '.obj')
                out_path = os.path.join(tempfile.gettempdir(), f'houdini_export{ext}')
            else:
                out_path = output
            
            try:
                geo.saveToFile(out_path)
                
                # Gather stats
                stats = {
                    'points': geo.intrinsicValue('pointcount'),
                    'prims': geo.intrinsicValue('primitivecount'),
                    'vertices': geo.intrinsicValue('vertexcount'),
                }
                
                bbox = geo.boundingBox()
                stats['bounds'] = {
                    'min': list(bbox.minvec()),
                    'max': list(bbox.maxvec()),
                    'center': list(bbox.center()),
                    'size': list(bbox.sizevec()),
                }
                
                return {
                    'success': True,
                    'output': out_path,
                    'format': format,
                    'stats': stats,
                }
            except Exception as e:
                return {'error': str(e)}
        
        self.send_json(export())
    
    def handle_render_snapshot(self, body: dict):
        """Render a snapshot from viewport or Karma."""
        render_type = body.get('type', 'viewport')  # 'viewport' or 'karma'
        output = body.get('output')
        resolution = body.get('resolution', [1920, 1080])
        camera = body.get('camera')  # optional camera path
        lop_node = body.get('lop_node')  # for karma, which LOP to render
        
        @require_main_thread
        def render():
            import tempfile
            import os
            
            # Generate output path if not specified
            if not output:
                out_path = os.path.join(tempfile.gettempdir(), 'houdini_snapshot.png')
            else:
                out_path = output
            
            if render_type == 'viewport':
                # Capture from scene viewer
                desktop = hou.ui.curDesktop()
                scene_viewer = None
                
                # Find a scene viewer
                for pane in desktop.panes():
                    for tab in pane.tabs():
                        if tab.type() == hou.paneTabType.SceneViewer:
                            scene_viewer = tab
                            break
                    if scene_viewer:
                        break
                
                if not scene_viewer:
                    return {'error': 'No scene viewer found'}
                
                try:
                    # Get the viewport
                    viewport = scene_viewer.curViewport()
                    
                    # Create flipbook settings for single frame capture
                    settings = scene_viewer.flipbookSettings()
                    settings.output(out_path)
                    settings.frameRange((hou.frame(), hou.frame()))
                    settings.resolution(resolution)
                    
                    # Capture
                    scene_viewer.flipbook(viewport, settings)
                    
                    return {
                        'success': True,
                        'output': out_path,
                        'type': 'viewport',
                        'frame': hou.frame(),
                        'resolution': resolution,
                    }
                except Exception as e:
                    return {'error': f'Viewport capture failed: {str(e)}'}
            
            elif render_type == 'karma':
                # Render via Karma
                if not lop_node:
                    # Try to find a karma node in /stage
                    stage = hou.node('/stage')
                    if stage:
                        for child in stage.children():
                            if 'karma' in child.type().name().lower():
                                lop_node = child.path()
                                break
                
                if not lop_node:
                    return {'error': 'No LOP node specified and no Karma node found in /stage'}
                
                node = hou.node(lop_node)
                if not node:
                    return {'error': f'LOP node not found: {lop_node}'}
                
                try:
                    # Check if it's a USD Render ROP or similar
                    if hasattr(node, 'render'):
                        # Set output
                        if node.parm('picture'):
                            node.parm('picture').set(out_path)
                        if node.parm('resolutionx'):
                            node.parm('resolutionx').set(resolution[0])
                        if node.parm('resolutiony'):
                            node.parm('resolutiony').set(resolution[1])
                        
                        # Render single frame
                        node.render()
                        
                        return {
                            'success': True,
                            'output': out_path,
                            'type': 'karma',
                            'node': lop_node,
                            'frame': hou.frame(),
                            'resolution': resolution,
                        }
                    else:
                        return {'error': f'Node {lop_node} does not support rendering'}
                except Exception as e:
                    return {'error': f'Karma render failed: {str(e)}'}
            
            else:
                return {'error': f'Unknown render type: {render_type}'}
        
        self.send_json(render())
    
    def handle_render_flipbook(self, body: dict):
        """Render a flipbook (frame sequence) from viewport."""
        output = body.get('output')  # Should include $F or frame pattern
        frame_range = body.get('frame_range')  # [start, end]
        resolution = body.get('resolution', [1920, 1080])
        
        if not frame_range:
            self.send_error_json(400, "Missing 'frame_range' parameter")
            return
        
        @require_main_thread
        def flipbook():
            import tempfile
            import os
            
            # Generate output path if not specified
            if not output:
                out_dir = tempfile.mkdtemp(prefix='houdini_flipbook_')
                out_path = os.path.join(out_dir, 'frame_$F4.png')
            else:
                out_path = output
            
            desktop = hou.ui.curDesktop()
            scene_viewer = None
            
            for pane in desktop.panes():
                for tab in pane.tabs():
                    if tab.type() == hou.paneTabType.SceneViewer:
                        scene_viewer = tab
                        break
                if scene_viewer:
                    break
            
            if not scene_viewer:
                return {'error': 'No scene viewer found'}
            
            try:
                viewport = scene_viewer.curViewport()
                settings = scene_viewer.flipbookSettings()
                settings.output(out_path)
                settings.frameRange((frame_range[0], frame_range[1]))
                settings.resolution(resolution)
                
                scene_viewer.flipbook(viewport, settings)
                
                return {
                    'success': True,
                    'output': out_path,
                    'frame_range': frame_range,
                    'resolution': resolution,
                }
            except Exception as e:
                return {'error': f'Flipbook failed: {str(e)}'}
        
        self.send_json(flipbook())
    
    def handle_batch(self, body: dict):
        """Execute multiple operations atomically."""
        operations = body.get('operations', [])
        
        if not operations:
            self.send_error_json(400, "No operations provided")
            return
        
        @require_main_thread
        def execute_batch():
            results = []
            
            with hou.undos.group("MCP: Batch Operation"):
                for i, op in enumerate(operations):
                    op_type = op.get('type')
                    op_args = op.get('args', {})
                    
                    try:
                        # Map operation types to internal handlers
                        if op_type == 'create':
                            result = self._batch_create(op_args)
                        elif op_type == 'connect':
                            result = self._batch_connect(op_args)
                        elif op_type == 'set_parm':
                            result = self._batch_set_parm(op_args)
                        elif op_type == 'set_flag':
                            result = self._batch_set_flag(op_args)
                        else:
                            result = {'error': f'Unknown operation type: {op_type}'}
                        
                        results.append({'index': i, 'type': op_type, 'result': result})
                    except Exception as e:
                        results.append({'index': i, 'type': op_type, 'error': str(e)})
            
            return {'results': results, 'count': len(results)}
        
        self.send_json(execute_batch())
    
    # =========================================================================
    # Internal Helpers
    # =========================================================================
    
    def _serialize_node(self, node: 'hou.Node', include_parms: bool = False) -> dict:
        """Serialize a node to a dictionary."""
        data = {
            'path': node.path(),
            'name': node.name(),
            'type': node.type().name(),
            'type_label': node.type().description(),
            'category': node.type().category().name(),
            'color': list(node.color().rgb()),
            'position': list(node.position()),
            'comment': node.comment(),
            'inputs': [
                {'name': c.name(), 'path': c.path()} if c else None
                for c in node.inputs()
            ],
            'outputs': [
                {'name': c.name(), 'path': c.path()}
                for c in node.outputs()
            ],
            'errors': [e.text() for e in node.errors()] if hasattr(node, 'errors') else [],
            'warnings': [w.text() for w in node.warnings()] if hasattr(node, 'warnings') else [],
        }
        
        # Flags (if applicable)
        flags = {}
        if hasattr(node, 'isDisplayFlagSet'):
            flags['display'] = node.isDisplayFlagSet()
        if hasattr(node, 'isRenderFlagSet'):
            flags['render'] = node.isRenderFlagSet()
        if hasattr(node, 'isBypassed'):
            flags['bypass'] = node.isBypassed()
        if hasattr(node, 'isTemplateFlagSet'):
            flags['template'] = node.isTemplateFlagSet()
        data['flags'] = flags
        
        if include_parms:
            data['parms'] = [
                self._serialize_parm(p) for p in node.parms() if not p.isAtDefault()
            ]
        
        return data
    
    def _serialize_parm(self, parm: 'hou.Parm') -> dict:
        """Serialize a parameter to a dictionary."""
        tmpl = parm.parmTemplate()
        data = {
            'name': parm.name(),
            'label': tmpl.label(),
            'type': tmpl.type().name(),
            'value': parm.eval(),
            'is_expression': parm.isExpression(),
            'is_at_default': parm.isAtDefault(),
        }
        
        if parm.isExpression():
            try:
                data['expression'] = parm.expression()
                data['expression_language'] = parm.expressionLanguage().name()
            except:
                pass
        
        return data
    
    def _serialize_parm_tuple(self, parm_tuple: 'hou.ParmTuple') -> dict:
        """Serialize a parameter tuple to a dictionary."""
        tmpl = parm_tuple.parmTemplate()
        return {
            'name': parm_tuple.name(),
            'label': tmpl.label(),
            'type': 'tuple',
            'size': len(parm_tuple),
            'value': list(parm_tuple.eval()),
        }
    
    def _batch_create(self, args: dict) -> dict:
        """Internal create for batch operations."""
        parent = hou.node(args.get('parent', '/obj'))
        if not parent:
            return {'error': 'Parent not found'}
        node = parent.createNode(args['type'], node_name=args.get('name'))
        if args.get('position'):
            node.setPosition(hou.Vector2(args['position']))
        return {'path': node.path(), 'name': node.name()}
    
    def _batch_connect(self, args: dict) -> dict:
        """Internal connect for batch operations."""
        from_node = hou.node(args['from'])
        to_node = hou.node(args['to'])
        if not from_node or not to_node:
            return {'error': 'Node not found'}
        to_node.setInput(args.get('to_input', 0), from_node, args.get('from_output', 0))
        return {'success': True}
    
    def _batch_set_parm(self, args: dict) -> dict:
        """Internal set_parm for batch operations."""
        node = hou.node(args['path'])
        if not node:
            return {'error': 'Node not found'}
        parm = node.parm(args['parm'])
        if not parm:
            return {'error': 'Parameter not found'}
        parm.set(args['value'])
        return {'value': parm.eval()}
    
    def _batch_set_flag(self, args: dict) -> dict:
        """Internal set_flag for batch operations."""
        node = hou.node(args['path'])
        if not node:
            return {'error': 'Node not found'}
        flag = args['flag']
        value = args.get('value', True)
        if flag == 'display':
            node.setDisplayFlag(value)
        elif flag == 'render':
            node.setRenderFlag(value)
        elif flag == 'bypass':
            node.bypass(value)
        return {'success': True}
    
    # =========================================================================
    # Response Helpers
    # =========================================================================
    
    def send_json(self, data: Any):
        """Send JSON response."""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode('utf-8'))
    
    def send_error_json(self, code: int, message: str):
        """Send JSON error response."""
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({'error': message}).encode('utf-8'))
    
    def log_message(self, format: str, *args):
        """Suppress default logging, use Houdini console instead."""
        if IN_HOUDINI:
            print(f"[HoudiniBridge] {format % args}")


# =============================================================================
# Server Management
# =============================================================================

_server_instance: Optional[HTTPServer] = None
_server_thread: Optional[threading.Thread] = None


def start_bridge(port: int = 8765, host: str = '127.0.0.1') -> HTTPServer:
    """
    Start the Houdini bridge server.
    
    Args:
        port: Port to listen on (default: 8765)
        host: Host to bind to (default: 127.0.0.1 for localhost only)
    
    Returns:
        HTTPServer instance
    """
    global _server_instance, _server_thread
    
    if _server_instance is not None:
        print(f"[HoudiniBridge] Server already running on port {port}")
        return _server_instance
    
    _server_instance = HTTPServer((host, port), HoudiniBridgeHandler)
    _server_thread = threading.Thread(target=_server_instance.serve_forever, daemon=True)
    _server_thread.start()
    
    print(f"[HoudiniBridge] Server started on http://{host}:{port}")
    print(f"[HoudiniBridge] Endpoints: /ping, /scene/info, /node/get, /node/tree, ...")
    
    return _server_instance


def stop_bridge():
    """Stop the bridge server."""
    global _server_instance, _server_thread
    
    if _server_instance is not None:
        _server_instance.shutdown()
        _server_instance = None
        _server_thread = None
        print("[HoudiniBridge] Server stopped")


def is_running() -> bool:
    """Check if the bridge server is running."""
    return _server_instance is not None


# =============================================================================
# Shelf Tool Entry Point
# =============================================================================

def toggle_bridge():
    """Toggle the bridge server on/off (for shelf tool)."""
    if is_running():
        stop_bridge()
    else:
        start_bridge()


# Auto-start when loaded in Houdini (optional)
if __name__ == '__main__' and IN_HOUDINI:
    start_bridge()
