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
            # PDG/TOPs
            '/pdg/status': self.handle_pdg_status,
            '/pdg/workitems': self.handle_pdg_workitems,
            # USD/LOPs
            '/lop/stage/info': self.handle_lop_stage_info,
            '/lop/prim/get': self.handle_lop_prim_get,
            '/lop/layer/info': self.handle_lop_layer_info,
            '/lop/prim/search': self.handle_lop_prim_search,
            # HDA
            '/hda/get': self.handle_hda_get,
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
            # PDG/TOPs
            '/pdg/cook': self.handle_pdg_cook,
            '/pdg/dirty': self.handle_pdg_dirty,
            '/pdg/cancel': self.handle_pdg_cancel,
            # USD/LOPs
            '/lop/import': self.handle_lop_import,
            # HDA
            '/hda/create': self.handle_hda_create,
            '/hda/install': self.handle_hda_install,
            '/hda/reload': self.handle_hda_reload,
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
                'memory_usage_mb': self._get_memory_mb(),
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
                'memory_usage_mb': self._get_memory_mb(),
            }
        
        self.send_json(get_status())
    
    def handle_hda_list(self, params: dict):
        """List available HDA definitions."""
        category = params.get('category')
        
        @require_main_thread
        def list_hdas():
            hdas = []
            for definition in hou.hda.loadedFiles():
                for defn in hou.hda.definitionsInFile(definition):
                    cat_name = defn.nodeTypeCategory().name()
                    if category and cat_name != category:
                        continue
                    hdas.append({
                        'name': defn.nodeTypeName(),
                        'label': defn.description(),
                        'category': cat_name,
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
    
    @staticmethod
    def _get_memory_mb() -> float:
        """Get current process memory usage in MB."""
        try:
            import resource
            # ru_maxrss is in KB on Linux
            return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        except Exception:
            return -1

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
    
    def _serialize_workitem(self, wi) -> dict:
        """Serialize a PDG work item to a dictionary."""
        state_raw = str(wi.state)
        # Strip enum prefix like "workItemState.CookedSuccess" → "CookedSuccess"
        state_str = state_raw.rsplit('.', 1)[-1] if '.' in state_raw else state_raw

        data = {
            'name': wi.name,
            'index': wi.index,
            'state': state_str,
        }

        # Attributes
        try:
            attrs = {}
            for attr_name in wi.attribNames:
                try:
                    vals = wi.attribValue(attr_name)
                    attrs[attr_name] = vals
                except Exception:
                    pass
            if attrs:
                data['attributes'] = attrs
        except Exception:
            pass

        # Output files
        try:
            outputs = []
            for result in wi.resultData:
                outputs.append({
                    'path': result.localize(),
                    'tag': result.tag,
                })
            if outputs:
                data['output_files'] = outputs
        except Exception:
            pass

        return data

    def _serialize_usd_prim(self, prim, include_attrs: bool = True) -> dict:
        """Serialize a USD prim to a dictionary."""
        data = {
            'path': str(prim.GetPath()),
            'type': str(prim.GetTypeName()),
            'active': prim.IsActive(),
        }

        # Kind
        try:
            from pxr import Kind, Usd
            model = Usd.ModelAPI(prim)
            data['kind'] = model.GetKind()
        except Exception:
            data['kind'] = ''

        # Purpose
        try:
            from pxr import UsdGeom
            imageable = UsdGeom.Imageable(prim)
            data['purpose'] = imageable.GetPurposeAttr().Get()
        except Exception:
            data['purpose'] = ''

        # Children
        children = []
        for child in prim.GetChildren():
            children.append({
                'path': str(child.GetPath()),
                'type': str(child.GetTypeName()),
            })
        data['children'] = children

        # Has references
        try:
            data['has_references'] = prim.HasAuthoredReferences()
        except Exception:
            data['has_references'] = False

        # Attributes
        if include_attrs:
            attributes = []
            for attr in prim.GetAttributes():
                attr_data = {
                    'name': attr.GetName(),
                    'type': str(attr.GetTypeName()),
                }
                try:
                    val = attr.Get()
                    # Convert non-serializable types to string
                    if val is not None:
                        try:
                            json.dumps(val)
                            attr_data['value'] = val
                        except (TypeError, ValueError):
                            attr_data['value'] = str(val)
                except Exception:
                    attr_data['value'] = None
                attributes.append(attr_data)
            data['attributes'] = attributes

        return data

    def _serialize_hda_definition(self, definition) -> dict:
        """Serialize an HDA definition to a dictionary."""
        data = {
            'type_name': definition.nodeTypeName(),
            'description': definition.description(),
            'library_file': definition.libraryFilePath(),
            'category': definition.nodeTypeCategory().name(),
        }

        try:
            data['version'] = definition.version()
        except Exception:
            data['version'] = ''

        try:
            data['is_current'] = definition.isCurrent()
        except Exception:
            data['is_current'] = None

        try:
            data['is_preferred'] = definition.isPreferred()
        except Exception:
            data['is_preferred'] = None

        try:
            data['min_inputs'] = definition.minNumInputs()
            data['max_inputs'] = definition.maxNumInputs()
        except Exception:
            pass

        try:
            data['min_outputs'] = definition.minNumOutputs()
            data['max_outputs'] = definition.maxNumOutputs()
        except Exception:
            pass

        # Sections
        try:
            sections = []
            for name in definition.sections():
                sections.append(name)
            data['sections'] = sections
        except Exception:
            data['sections'] = []

        # Embedded help
        try:
            help_text = definition.embeddedHelp()
            if help_text:
                data['help'] = help_text[:2000]  # Truncate long help
        except Exception:
            pass

        return data

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
    # PDG/TOPs Handlers
    # =========================================================================

    def handle_pdg_status(self, params: dict):
        """Get PDG graph context state and work item counts."""
        path = params.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def get_status():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                ctx = node.getPDGGraphContext()
            except AttributeError:
                return {'error': f'Node is not a TOP network or node: {path}'}

            if ctx is None:
                return {'error': f'No PDG graph context found at: {path}'}

            try:
                cook_state = str(ctx.cookState())
            except Exception:
                cook_state = 'unknown'

            # Gather work item stats across all nodes in the graph
            state_counts = {
                'waiting': 0,
                'uncooked': 0,
                'cooking': 0,
                'cooked': 0,
                'success': 0,
                'failed': 0,
                'cancelled': 0,
                'total': 0,
            }

            # Map PDG state enum names to our bucket names
            _state_map = {
                'waiting': 'waiting',
                'uncooked': 'uncooked',
                'cooking': 'cooking',
                'cookedsuccess': 'success',
                'cooked': 'cooked',
                'cookedfail': 'failed',
                'cookedcancel': 'cancelled',
            }

            top_nodes = []
            try:
                # Iterate TOP nodes within this context
                child_cat = node.type().childTypeCategory()
                is_top_container = child_cat is not None and child_cat.name() == 'Top'
                container = node if is_top_container else node.parent()
                if container:
                    for child in container.children():
                        try:
                            pdg_node = child.getPDGNode()
                            if pdg_node:
                                top_nodes.append(child.path())
                                for wi in pdg_node.workItems:
                                    raw = str(wi.state)
                                    key = raw.rsplit('.', 1)[-1].lower() if '.' in raw else raw.lower()
                                    bucket = _state_map.get(key)
                                    state_counts['total'] += 1
                                    if bucket and bucket in state_counts:
                                        state_counts[bucket] += 1
                        except Exception:
                            continue
            except Exception:
                pass

            return {
                'path': path,
                'cook_state': cook_state,
                'work_item_counts': state_counts,
                'top_node_count': len(top_nodes),
            }

        self.send_json(get_status())

    def handle_pdg_workitems(self, params: dict):
        """Get work items from a TOP node."""
        path = params.get('path')
        state_filter = params.get('state')

        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def get_workitems():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                pdg_node = node.getPDGNode()
            except AttributeError:
                return {'error': f'Node is not a TOP node: {path}'}

            if pdg_node is None:
                return {'error': f'No PDG node found at: {path}'}

            # Map filter names to PDG state suffixes
            _filter_map = {
                'waiting': 'waiting',
                'uncooked': 'uncooked',
                'cooking': 'cooking',
                'cooked': 'cooked',
                'success': 'cookedsuccess',
                'failed': 'cookedfail',
                'cancelled': 'cookedcancel',
            }

            items = []
            for wi in pdg_node.workItems:
                if state_filter:
                    raw = str(wi.state)
                    key = raw.rsplit('.', 1)[-1].lower() if '.' in raw else raw.lower()
                    target = _filter_map.get(state_filter.lower(), state_filter.lower())
                    if key != target:
                        continue
                items.append(self._serialize_workitem(wi))

            return {
                'path': path,
                'work_items': items,
                'count': len(items),
            }

        self.send_json(get_workitems())

    def handle_pdg_cook(self, body: dict):
        """Cook a PDG/TOP graph (non-blocking)."""
        path = body.get('path')
        tops_only = body.get('tops_only', True)

        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def cook():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                ctx = node.getPDGGraphContext()
            except AttributeError:
                return {'error': f'Node is not a TOP network or node: {path}'}

            if ctx is None:
                return {'error': f'No PDG graph context found at: {path}'}

            try:
                if tops_only:
                    node.executeGraph(False, False, False, False)
                else:
                    ctx.cook(block=False)
                return {
                    'success': True,
                    'path': path,
                    'message': 'Cook initiated (non-blocking). Use houdini_pdg_status to poll progress.',
                }
            except Exception as e:
                return {'error': f'Failed to cook PDG graph: {str(e)}'}

        self.send_json(cook())

    def handle_pdg_dirty(self, body: dict):
        """Dirty PDG work items."""
        path = body.get('path')
        dirty_all = body.get('dirty_all', False)

        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def dirty():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                ctx = node.getPDGGraphContext()
            except AttributeError:
                return {'error': f'Node is not a TOP network or node: {path}'}

            if ctx is None:
                return {'error': f'No PDG graph context found at: {path}'}

            try:
                if dirty_all:
                    # Dirty all TOP nodes in the network
                    child_cat = node.type().childTypeCategory()
                    is_top_container = child_cat is not None and child_cat.name() == 'Top'
                    container = node if is_top_container else node.parent()
                    if container:
                        for child in container.children():
                            try:
                                child.dirtyAllTasks(False)
                            except Exception:
                                continue
                else:
                    node.dirtyAllTasks(False)
                return {'success': True, 'path': path, 'dirty_all': dirty_all}
            except Exception as e:
                return {'error': f'Failed to dirty PDG tasks: {str(e)}'}

        self.send_json(dirty())

    def handle_pdg_cancel(self, body: dict):
        """Cancel a PDG cook."""
        path = body.get('path')

        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def cancel():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                ctx = node.getPDGGraphContext()
            except AttributeError:
                return {'error': f'Node is not a TOP network or node: {path}'}

            if ctx is None:
                return {'error': f'No PDG graph context found at: {path}'}

            try:
                ctx.cancelCook()
                return {'success': True, 'path': path}
            except Exception as e:
                return {'error': f'Failed to cancel PDG cook: {str(e)}'}

        self.send_json(cancel())

    # =========================================================================
    # USD/Solaris/LOP Handlers
    # =========================================================================

    def handle_lop_stage_info(self, params: dict):
        """Get USD stage information from a LOP node."""
        path = params.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def get_info():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                stage = node.stage()
            except AttributeError:
                return {'error': f'Node is not a LOP node: {path}'}

            if stage is None:
                return {'error': f'No USD stage at: {path}'}

            # Count prims
            prim_count = 0
            root_prims = []
            for prim in stage.Traverse():
                prim_count += 1

            for prim in stage.GetPseudoRoot().GetChildren():
                root_prims.append({
                    'path': str(prim.GetPath()),
                    'type': str(prim.GetTypeName()),
                })

            # Default prim
            default_prim = None
            try:
                dp = stage.GetDefaultPrim()
                if dp:
                    default_prim = str(dp.GetPath())
            except Exception:
                pass

            # Layer stack
            layer_count = 0
            try:
                layer_count = len(stage.GetLayerStack())
            except Exception:
                pass

            # Time codes
            start_time = None
            end_time = None
            try:
                if stage.HasAuthoredTimeCodeRange():
                    start_time = stage.GetStartTimeCode()
                    end_time = stage.GetEndTimeCode()
            except Exception:
                pass

            return {
                'path': path,
                'prim_count': prim_count,
                'root_prims': root_prims,
                'default_prim': default_prim,
                'layer_count': layer_count,
                'start_time_code': start_time,
                'end_time_code': end_time,
            }

        self.send_json(get_info())

    def handle_lop_prim_get(self, params: dict):
        """Get detailed info about a USD prim."""
        path = params.get('path')
        prim_path = params.get('prim_path')
        include_attrs = params.get('include_attrs', 'true').lower() == 'true'

        if not path or not prim_path:
            self.send_error_json(400, "Missing 'path' or 'prim_path' parameter")
            return

        @require_main_thread
        def get_prim():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                stage = node.stage()
            except AttributeError:
                return {'error': f'Node is not a LOP node: {path}'}

            if stage is None:
                return {'error': f'No USD stage at: {path}'}

            prim = stage.GetPrimAtPath(prim_path)
            if not prim or not prim.IsValid():
                return {'error': f'Prim not found: {prim_path}'}

            return self._serialize_usd_prim(prim, include_attrs=include_attrs)

        self.send_json(get_prim())

    def handle_lop_layer_info(self, params: dict):
        """Get USD layer stack information."""
        path = params.get('path')
        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def get_layers():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                stage = node.stage()
            except AttributeError:
                return {'error': f'Node is not a LOP node: {path}'}

            if stage is None:
                return {'error': f'No USD stage at: {path}'}

            layers = []
            try:
                for layer in stage.GetLayerStack():
                    layer_info = {
                        'identifier': layer.identifier,
                        'sublayer_paths': list(layer.subLayerPaths),
                    }
                    # Root prims authored in this layer
                    try:
                        layer_info['root_prim_paths'] = [
                            str(p.path) for p in layer.rootPrims
                        ]
                    except Exception:
                        layer_info['root_prim_paths'] = []
                    layers.append(layer_info)
            except Exception as e:
                return {'error': f'Failed to read layer stack: {str(e)}'}

            # Active layer
            active_layer_id = None
            try:
                active_layer_id = node.activeLayer().identifier
            except Exception:
                pass

            return {
                'path': path,
                'active_layer': active_layer_id,
                'layers': layers,
                'count': len(layers),
            }

        self.send_json(get_layers())

    def handle_lop_prim_search(self, params: dict):
        """Search for USD prims by path pattern and/or type."""
        path = params.get('path')
        pattern = params.get('pattern', '/**')
        type_name = params.get('type_name')

        if not path:
            self.send_error_json(400, "Missing 'path' parameter")
            return

        @require_main_thread
        def search():
            node = hou.node(path)
            if not node:
                return {'error': f'Node not found: {path}'}

            try:
                stage = node.stage()
            except AttributeError:
                return {'error': f'Node is not a LOP node: {path}'}

            if stage is None:
                return {'error': f'No USD stage at: {path}'}

            results = []
            try:
                rule = hou.LopSelectionRule()
                rule.setPathPattern(pattern)
                expanded = rule.expandedPaths(lopnode=node)

                for prim_path in expanded:
                    prim = stage.GetPrimAtPath(str(prim_path))
                    if not prim or not prim.IsValid():
                        continue

                    prim_type = str(prim.GetTypeName())
                    if type_name and prim_type != type_name:
                        continue

                    kind = ''
                    try:
                        from pxr import UsdGeom
                        model = pxr.Usd.ModelAPI(prim)
                        kind = model.GetKind()
                    except Exception:
                        pass

                    results.append({
                        'path': str(prim.GetPath()),
                        'type': prim_type,
                        'kind': kind,
                    })
            except Exception as e:
                return {'error': f'Search failed: {str(e)}'}

            return {
                'node_path': path,
                'pattern': pattern,
                'results': results,
                'count': len(results),
            }

        self.send_json(search())

    def handle_lop_import(self, body: dict):
        """Import a USD file into a LOP network."""
        path = body.get('path')
        file_path = body.get('file')
        method = body.get('method', 'reference')
        prim_path = body.get('prim_path')

        if not path or not file_path:
            self.send_error_json(400, "Missing 'path' or 'file' parameter")
            return

        if method not in ('reference', 'sublayer'):
            self.send_error_json(400, "method must be 'reference' or 'sublayer'")
            return

        @require_main_thread
        def do_import():
            parent = hou.node(path)
            if not parent:
                return {'error': f'Parent node not found: {path}'}

            try:
                with hou.undos.group("MCP: LOP Import"):
                    node = parent.createNode(method)

                    # Set file path
                    file_parm = node.parm('filepath') or node.parm('filepath1') or node.parm('fileName')
                    if file_parm:
                        file_parm.set(file_path)

                    # Set prim path if applicable
                    if prim_path and method == 'reference':
                        prim_parm = node.parm('primpath') or node.parm('primpath1')
                        if prim_parm:
                            prim_parm.set(prim_path)

                    parent.layoutChildren()

                    return {
                        'success': True,
                        'node_path': node.path(),
                        'method': method,
                        'file': file_path,
                    }
            except Exception as e:
                return {'error': f'Import failed: {str(e)}'}

        self.send_json(do_import())

    # =========================================================================
    # HDA Management Handlers
    # =========================================================================

    def handle_hda_get(self, params: dict):
        """Get detailed HDA definition info."""
        node_type = params.get('node_type')
        category = params.get('category')

        if not node_type:
            self.send_error_json(400, "Missing 'node_type' parameter")
            return

        @require_main_thread
        def get_hda():
            try:
                cat = hou.nodeTypeCategories().get(category) if category else None

                # Search through node type categories
                definition = None
                if cat:
                    nt = cat.nodeTypes().get(node_type)
                    if nt:
                        definition = nt.definition()
                else:
                    for cat_name, cat_obj in hou.nodeTypeCategories().items():
                        nt = cat_obj.nodeTypes().get(node_type)
                        if nt and nt.definition():
                            definition = nt.definition()
                            break

                if not definition:
                    return {'error': f'HDA definition not found: {node_type}'}

                return self._serialize_hda_definition(definition)
            except Exception as e:
                return {'error': f'Failed to get HDA info: {str(e)}'}

        self.send_json(get_hda())

    def handle_hda_create(self, body: dict):
        """Create an HDA from a node."""
        node_path = body.get('node_path')
        name = body.get('name')
        label = body.get('label')
        file_path = body.get('file_path')
        version = body.get('version')
        min_inputs = body.get('min_inputs')
        max_inputs = body.get('max_inputs')

        if not all([node_path, name, label, file_path]):
            self.send_error_json(400, "Missing required parameters: node_path, name, label, file_path")
            return

        @require_main_thread
        def create_hda():
            node = hou.node(node_path)
            if not node:
                return {'error': f'Node not found: {node_path}'}

            try:
                with hou.undos.group("MCP: Create HDA"):
                    hda_node = node.createDigitalAsset(
                        name=name,
                        hda_file_name=file_path,
                        description=label,
                    )

                    definition = hda_node.type().definition()

                    if version and definition:
                        definition.setVersion(version)
                    if min_inputs is not None and definition:
                        definition.setMinNumInputs(min_inputs)
                    if max_inputs is not None and definition:
                        definition.setMaxNumInputs(max_inputs)

                    return {
                        'success': True,
                        'file_path': file_path,
                        'type_name': name,
                        'node_path': hda_node.path(),
                    }
            except Exception as e:
                return {'error': f'Failed to create HDA: {str(e)}'}

        self.send_json(create_hda())

    def handle_hda_install(self, body: dict):
        """Install an HDA file."""
        file_path = body.get('file_path')

        if not file_path:
            self.send_error_json(400, "Missing 'file_path' parameter")
            return

        @require_main_thread
        def install():
            try:
                hou.hda.installFile(file_path)

                # Get installed definitions
                definitions = []
                for defn in hou.hda.definitionsInFile(file_path):
                    definitions.append({
                        'name': defn.nodeTypeName(),
                        'category': defn.nodeTypeCategory().name(),
                        'description': defn.description(),
                    })

                return {
                    'success': True,
                    'file_path': file_path,
                    'definitions': definitions,
                    'count': len(definitions),
                }
            except Exception as e:
                return {'error': f'Failed to install HDA: {str(e)}'}

        self.send_json(install())

    def handle_hda_reload(self, body: dict):
        """Reload HDA definitions."""
        file_path = body.get('file_path')

        @require_main_thread
        def reload():
            try:
                if file_path:
                    hou.hda.reloadFile(file_path)
                    return {'success': True, 'file_path': file_path}
                else:
                    hou.hda.reloadAllFiles(rescan=True)
                    return {'success': True, 'message': 'All HDA files reloaded'}
            except Exception as e:
                return {'error': f'Failed to reload HDA: {str(e)}'}

        self.send_json(reload())

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
