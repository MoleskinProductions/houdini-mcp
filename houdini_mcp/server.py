"""
Houdini MCP Server

This is the external MCP server that Claude agents communicate with.
It forwards requests to the Houdini bridge server running inside Houdini.

Usage:
    # stdio transport (Claude Code / Claude Desktop)
    python -m houdini_mcp.server

    # Streamable HTTP transport (Claude web app via tunnel)
    python -m houdini_mcp.server --http --port 8080

    Or configure in Claude Desktop / Claude Code config:
    {
        "mcpServers": {
            "houdini": {
                "command": "python",
                "args": ["-m", "houdini_mcp.server"]
            }
        }
    }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from typing import Any

import httpx
from mcp.server import Server
from mcp.types import (
    AudioContent,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    ListToolsResult,
    ResourceLink,
    TextContent,
    Tool,
)

# MCP content union type used by CallToolResult
ContentItem = TextContent | ImageContent | AudioContent | ResourceLink | EmbeddedResource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('houdini-mcp')

# Configuration
HOUDINI_BRIDGE_URL = os.environ.get('HOUDINI_BRIDGE_URL', 'http://127.0.0.1:8765')
TIMEOUT = float(os.environ.get('HOUDINI_TIMEOUT', '30.0'))

# Initialize MCP server
server = Server("houdini-mcp")


# =============================================================================
# Bridge Communication
# =============================================================================

async def call_bridge(
    method: str,
    endpoint: str,
    params: dict | None = None,
    body: dict | None = None
) -> Any:
    """
    Make a request to the Houdini bridge.

    Args:
        method: HTTP method (GET or POST)
        endpoint: API endpoint (e.g., '/scene/info')
        params: Query parameters for GET requests
        body: JSON body for POST requests

    Returns:
        Response data as dictionary

    Raises:
        Exception: If the request fails
    """
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        url = f"{HOUDINI_BRIDGE_URL}{endpoint}"

        if method == 'GET':
            response = await client.get(url, params=params)
        else:
            response = await client.post(url, json=body)

        response.raise_for_status()
        return response.json()


def format_result(data: Any) -> str:
    """Format result data as pretty JSON string."""
    return json.dumps(data, indent=2, default=str)


def error_result(message: str) -> list[ContentItem]:
    """Create an error result."""
    return [TextContent(type="text", text=f"ERROR: {message}")]


# =============================================================================
# Tool Definitions
# =============================================================================

TOOLS = [
    # --- Read Operations ---
    Tool(
        name="houdini_ping",
        description="Check if Houdini is running and get version info. Use this first to verify connectivity.",
        inputSchema={
            "type": "object",
            "properties": {},
        }
    ),
    Tool(
        name="houdini_scene_info",
        description="Get information about the current Houdini scene including hip file path, frame range, FPS, and node counts by context.",
        inputSchema={
            "type": "object",
            "properties": {},
        }
    ),
    Tool(
        name="houdini_node_get",
        description="Get detailed information about a specific node including its type, parameters, connections, flags, and any errors/warnings.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the node, e.g., '/obj/geo1' or '/obj/geo1/scatter1'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_node_tree",
        description="Get the hierarchical node tree structure from a root path. Useful for understanding scene organization.",
        inputSchema={
            "type": "object",
            "properties": {
                "root": {
                    "type": "string",
                    "description": "Root path to start from (default: /obj)",
                    "default": "/obj"
                },
                "depth": {
                    "type": "integer",
                    "description": "How many levels deep to traverse (default: 2)",
                    "default": 2
                }
            }
        }
    ),
    Tool(
        name="houdini_node_search",
        description="Search for nodes by name pattern or type.",
        inputSchema={
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Name pattern to search for (case-insensitive, partial match)",
                    "default": "*"
                },
                "type": {
                    "type": "string",
                    "description": "Filter by node type name (exact match)"
                },
                "root": {
                    "type": "string",
                    "description": "Root path to search from (default: /)",
                    "default": "/"
                }
            }
        }
    ),
    Tool(
        name="houdini_parm_get",
        description="Get parameter values from a node. Returns all non-default parameters if no specific parameter is requested.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the node"
                },
                "parm": {
                    "type": "string",
                    "description": "Specific parameter name (optional - returns all modified params if omitted)"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_cook_status",
        description="Get current cook/simulation status and memory usage.",
        inputSchema={
            "type": "object",
            "properties": {},
        }
    ),
    Tool(
        name="houdini_hda_list",
        description="List available HDA (Digital Asset) definitions.",
        inputSchema={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category (e.g., 'Sop', 'Object', 'Lop')"
                }
            }
        }
    ),

    # --- Write Operations ---
    Tool(
        name="houdini_node_create",
        description="Create a new node in Houdini. Returns the created node's path.",
        inputSchema={
            "type": "object",
            "properties": {
                "parent": {
                    "type": "string",
                    "description": "Parent node path where the new node will be created (default: /obj)",
                    "default": "/obj"
                },
                "type": {
                    "type": "string",
                    "description": "Node type to create (e.g., 'geo', 'null', 'scatter', 'grid', 'sphere')"
                },
                "name": {
                    "type": "string",
                    "description": "Optional name for the node (auto-generated if not provided)"
                },
                "position": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Optional [x, y] position in network editor"
                }
            },
            "required": ["type"]
        }
    ),
    Tool(
        name="houdini_node_delete",
        description="Delete a node from the scene.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the node to delete"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_node_rename",
        description="Rename a node.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the node"
                },
                "name": {
                    "type": "string",
                    "description": "New name for the node"
                }
            },
            "required": ["path", "name"]
        }
    ),
    Tool(
        name="houdini_node_connect",
        description="Connect two nodes together. Creates a wire from source output to destination input.",
        inputSchema={
            "type": "object",
            "properties": {
                "from": {
                    "type": "string",
                    "description": "Source node path"
                },
                "to": {
                    "type": "string",
                    "description": "Destination node path"
                },
                "from_output": {
                    "type": "integer",
                    "description": "Output index on source node (default: 0)",
                    "default": 0
                },
                "to_input": {
                    "type": "integer",
                    "description": "Input index on destination node (default: 0)",
                    "default": 0
                }
            },
            "required": ["from", "to"]
        }
    ),
    Tool(
        name="houdini_node_disconnect",
        description="Disconnect a node input.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Node path"
                },
                "input": {
                    "type": "integer",
                    "description": "Input index to disconnect (default: 0)",
                    "default": 0
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_node_flag",
        description="Set node flags (display, render, bypass, template, selectable).",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Node path"
                },
                "flag": {
                    "type": "string",
                    "enum": ["display", "render", "bypass", "template", "selectable"],
                    "description": "Flag to set"
                },
                "value": {
                    "type": "boolean",
                    "description": "Flag value (default: true)",
                    "default": True
                }
            },
            "required": ["path", "flag"]
        }
    ),
    Tool(
        name="houdini_node_layout",
        description="Auto-layout children of a network.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the network to layout (default: /obj)",
                    "default": "/obj"
                }
            }
        }
    ),
    Tool(
        name="houdini_parm_set",
        description="Set a parameter value on a node.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Node path"
                },
                "parm": {
                    "type": "string",
                    "description": "Parameter name"
                },
                "value": {
                    "description": "Value to set (number, string, boolean, or array for vector/color params)"
                }
            },
            "required": ["path", "parm", "value"]
        }
    ),
    Tool(
        name="houdini_parm_expression",
        description="Set an expression on a parameter (hscript or python).",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Node path"
                },
                "parm": {
                    "type": "string",
                    "description": "Parameter name"
                },
                "expression": {
                    "type": "string",
                    "description": "Expression to set (e.g., '$F' for current frame, 'sin($T)' for sine of time)"
                },
                "language": {
                    "type": "string",
                    "enum": ["hscript", "python"],
                    "description": "Expression language (default: hscript)",
                    "default": "hscript"
                }
            },
            "required": ["path", "parm", "expression"]
        }
    ),
    Tool(
        name="houdini_parm_revert",
        description="Revert a parameter to its default value.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Node path"
                },
                "parm": {
                    "type": "string",
                    "description": "Parameter name"
                }
            },
            "required": ["path", "parm"]
        }
    ),
    Tool(
        name="houdini_frame_set",
        description="Set the current frame.",
        inputSchema={
            "type": "object",
            "properties": {
                "frame": {
                    "type": "number",
                    "description": "Frame number to set"
                }
            },
            "required": ["frame"]
        }
    ),
    Tool(
        name="houdini_scene_save",
        description="Save the current scene.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Optional path to save to (uses current path if not specified)"
                }
            }
        }
    ),
    Tool(
        name="houdini_geo_export",
        description="Export geometry from a SOP node to a file. Returns the output path and geometry stats.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the SOP node with geometry"
                },
                "format": {
                    "type": "string",
                    "enum": ["obj", "gltf", "glb", "usd", "usda", "ply", "bgeo"],
                    "description": "Export format (default: obj)",
                    "default": "obj"
                },
                "output": {
                    "type": "string",
                    "description": "Optional output file path (auto-generated temp file if not specified)"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_render_snapshot",
        description="Capture a snapshot from the viewport or render with Karma. Returns the output image path.",
        inputSchema={
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["viewport", "karma"],
                    "description": "Render type: 'viewport' for quick viewport capture, 'karma' for Karma render",
                    "default": "viewport"
                },
                "output": {
                    "type": "string",
                    "description": "Output file path (auto-generated temp file if not specified)"
                },
                "resolution": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Resolution as [width, height] (default: [1920, 1080])",
                    "default": [1920, 1080]
                },
                "camera": {
                    "type": "string",
                    "description": "Optional camera path to render from"
                },
                "lop_node": {
                    "type": "string",
                    "description": "For Karma renders, path to the LOP/ROP node (auto-detected if not specified)"
                }
            }
        }
    ),
    Tool(
        name="houdini_render_flipbook",
        description="Render a flipbook (frame sequence) from the viewport.",
        inputSchema={
            "type": "object",
            "properties": {
                "frame_range": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Frame range as [start, end]"
                },
                "output": {
                    "type": "string",
                    "description": "Output path pattern with $F for frame number (auto-generated if not specified)"
                },
                "resolution": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Resolution as [width, height] (default: [1920, 1080])",
                    "default": [1920, 1080]
                }
            },
            "required": ["frame_range"]
        }
    ),
    Tool(
        name="houdini_batch",
        description="Execute multiple operations atomically with undo support. Operations: create, connect, set_parm, set_flag.",
        inputSchema={
            "type": "object",
            "properties": {
                "operations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["create", "connect", "set_parm", "set_flag"],
                                "description": "Operation type"
                            },
                            "args": {
                                "type": "object",
                                "description": "Operation arguments"
                            }
                        },
                        "required": ["type", "args"]
                    },
                    "description": "List of operations to execute"
                }
            },
            "required": ["operations"]
        }
    ),

    # --- PDG/TOPs Operations ---
    Tool(
        name="houdini_pdg_status",
        description="Get PDG/TOPs graph status including cook state and work item counts by state (waiting, cooking, success, fail). Use to monitor TOP network progress.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a TOP network or TOP node, e.g., '/tasks/topnet1'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_pdg_workitems",
        description="Get work items from a TOP node with their name, index, state, attributes, and output files.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a TOP node, e.g., '/tasks/topnet1/ropfetch1'"
                },
                "state": {
                    "type": "string",
                    "enum": ["waiting", "uncooked", "cooking", "cooked", "success", "failed", "cancelled"],
                    "description": "Optional filter by work item state"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_pdg_cook",
        description="Start cooking a PDG/TOP graph (non-blocking). Use houdini_pdg_status to poll progress after starting.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a TOP network or node"
                },
                "tops_only": {
                    "type": "boolean",
                    "description": "If true (default), generate and cook work items. If false, use full graph cook.",
                    "default": True
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_pdg_dirty",
        description="Dirty (invalidate) PDG work items so they will re-cook.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a TOP network or node"
                },
                "dirty_all": {
                    "type": "boolean",
                    "description": "If true, dirty all work items in the graph. If false (default), dirty only the specified node's tasks.",
                    "default": False
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_pdg_cancel",
        description="Cancel a running PDG/TOP cook.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the TOP network"
                }
            },
            "required": ["path"]
        }
    ),

    # --- USD/Solaris/LOP Operations ---
    Tool(
        name="houdini_lop_stage_info",
        description="Get USD stage information from a LOP node: prim count, root prims, default prim, layer count, and time code range.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a LOP node, e.g., '/stage/sublayer1'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_lop_prim_get",
        description="Get detailed info about a specific USD prim including type, kind, active status, children, and attributes.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the LOP node"
                },
                "prim_path": {
                    "type": "string",
                    "description": "USD prim path, e.g., '/world/geo'"
                },
                "include_attrs": {
                    "type": "boolean",
                    "description": "Whether to include attribute values (default: true)",
                    "default": True
                }
            },
            "required": ["path", "prim_path"]
        }
    ),
    Tool(
        name="houdini_lop_layer_info",
        description="Get USD layer stack information: layer identifiers, sublayer paths, and authored prims per layer.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a LOP node"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_lop_prim_search",
        description="Search for USD prims by path pattern and/or type name. Supports glob-style patterns like '/world/**'.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a LOP node"
                },
                "pattern": {
                    "type": "string",
                    "description": "Prim path pattern, e.g., '/world/**' or '/*'",
                    "default": "/**"
                },
                "type_name": {
                    "type": "string",
                    "description": "Optional USD type filter, e.g., 'Mesh', 'Xform', 'DistantLight'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_lop_import",
        description="Import a USD file into a LOP network by creating a reference or sublayer node.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Parent LOP network path, e.g., '/stage'"
                },
                "file": {
                    "type": "string",
                    "description": "Path to the USD file to import"
                },
                "method": {
                    "type": "string",
                    "enum": ["reference", "sublayer"],
                    "description": "Import method (default: reference)",
                    "default": "reference"
                },
                "prim_path": {
                    "type": "string",
                    "description": "Optional target prim path (for reference method)"
                }
            },
            "required": ["path", "file"]
        }
    ),

    # --- VGGT Pipeline Operations ---
    Tool(
        name="houdini_vggt_setup",
        description=(
            "Install all VGGT Toolkit HDAs into the current Houdini session and verify environment health. "
            "Call this once at the start of a session before using any other VGGT tools. "
            "Installs 7 HDAs: Reconstruct, Depth Fields, Mesher, Tracker, Texture Project, COLMAP Export, Dataset Export."
        ),
        inputSchema={
            "type": "object",
            "properties": {},
        }
    ),
    Tool(
        name="houdini_vggt_create_node",
        description=(
            "Create a VGGT pipeline node with the correct container and initial parameters. "
            "SOP modules (Reconstruct, Depth Fields, Mesher, Tracker, Texture Project) are created inside a geo container at /obj. "
            "Driver modules (COLMAP Export, Dataset Export) are created at /out. "
            "Optionally set initial parameters and connect to an upstream node."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "module": {
                    "type": "string",
                    "enum": [
                        "Reconstruct", "Depth Fields", "Mesher",
                        "Tracker", "Texture Project",
                        "COLMAP Export", "Dataset Export"
                    ],
                    "description": "Which VGGT module to create"
                },
                "name": {
                    "type": "string",
                    "description": "Optional name for the node (auto-generated if not provided)"
                },
                "parms": {
                    "type": "object",
                    "description": "Optional parameter values to set, e.g. {\"image_dir\": \"/path/to/images\"}"
                },
                "connect_to": {
                    "type": "string",
                    "description": "Optional node path to connect as input 0 (for SOP modules that accept input)"
                }
            },
            "required": ["module"]
        }
    ),
    Tool(
        name="houdini_vggt_execute",
        description=(
            "Trigger the Execute callback on a VGGT node, running GPU inference. "
            "This is equivalent to pressing the Execute button on the HDA. "
            "For Reconstruct, this runs camera solving and point cloud generation. "
            "For Depth Fields, this extracts depth maps. For Tracker, this tracks points. "
            "Returns the result.json contents and node status when complete. "
            "WARNING: This can take 1-10+ minutes depending on image count and GPU."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the VGGT node, e.g. '/obj/vggt_reconstruct/VGGT_Reconstruct1'"
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds (default: 600, max: 3600)",
                    "default": 600
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_vggt_pipeline_status",
        description=(
            "Get the status of all VGGT nodes in the current scene. "
            "Returns each node's path, type, execution status (ready/running/done/error/stale), "
            "key parameter values, connections, and whether results exist. "
            "Optionally filter to a specific node path."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Optional: filter to a specific node path"
                }
            }
        }
    ),
    Tool(
        name="houdini_vggt_read_results",
        description=(
            "Read output artifacts from a completed VGGT node. "
            "Returns contents of result.json, cameras.json, log.txt, manifest.json, and/or progress.json "
            "from the node's result directory. Also lists all files in the result directory."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Full path to the VGGT node"
                },
                "include": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["result", "cameras", "log", "manifest", "progress"]
                    },
                    "description": "Which artifacts to include (default: all)",
                    "default": ["result", "cameras", "log", "manifest", "progress"]
                }
            },
            "required": ["path"]
        }
    ),

    # --- Extraction Operations ---
    Tool(
        name="houdini_geo_info",
        description="Get geometry summary from a SOP node: point/prim/vertex counts, prim type breakdown, bounding box, attribute catalog by class, groups, and memory usage.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a SOP node with geometry, e.g., '/obj/geo1/OUT'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_attrib_read",
        description="Bulk-read attribute values from geometry. Returns inline JSON for small data or a file reference for large data (>1MB). Use houdini_geo_info first to discover available attributes.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a SOP node with geometry"
                },
                "attrib_class": {
                    "type": "string",
                    "enum": ["point", "prim", "vertex", "detail"],
                    "description": "Attribute class (default: point)",
                    "default": "point"
                },
                "attrib_name": {
                    "type": "string",
                    "description": "Name of the attribute to read, e.g., 'P', 'N', 'Cd'"
                },
                "start": {
                    "type": "integer",
                    "description": "Start index for partial read (default: 0)",
                    "default": 0
                },
                "count": {
                    "type": "integer",
                    "description": "Number of elements to read (-1 for all, default: -1)",
                    "default": -1
                }
            },
            "required": ["path", "attrib_name"]
        }
    ),
    Tool(
        name="houdini_aov_list",
        description="Get AOV (render pass) configurations from a LOP or ROP node. Returns AOV names, types, data formats, and sources. Tries USD RenderVar prims first, falls back to Karma ROP parameters.",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a LOP or ROP node, e.g., '/stage/karmarendersettings1' or '/out/karma1'"
                }
            },
            "required": ["path"]
        }
    ),
    Tool(
        name="houdini_camera_get",
        description="Get camera configuration including focal length, aperture, resolution, clipping planes, and world transform matrix (4x4 row-major).",
        inputSchema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to a camera node, e.g., '/obj/cam1'"
                }
            },
            "required": ["path"]
        }
    ),

    # --- HDA Management Operations ---
    Tool(
        name="houdini_hda_get",
        description="Get detailed HDA definition info: library file, version, description, help text, sections, inputs/outputs.",
        inputSchema={
            "type": "object",
            "properties": {
                "node_type": {
                    "type": "string",
                    "description": "HDA type name to look up, e.g., 'my_hda'"
                },
                "category": {
                    "type": "string",
                    "description": "Optional node category filter, e.g., 'Sop', 'Object', 'Lop'"
                }
            },
            "required": ["node_type"]
        }
    ),
    Tool(
        name="houdini_hda_create",
        description="Create an HDA (Houdini Digital Asset) from an existing node. Packages the node into a reusable .hda file.",
        inputSchema={
            "type": "object",
            "properties": {
                "node_path": {
                    "type": "string",
                    "description": "Path to the source node to package"
                },
                "name": {
                    "type": "string",
                    "description": "HDA type name"
                },
                "label": {
                    "type": "string",
                    "description": "Human-readable label/description"
                },
                "file_path": {
                    "type": "string",
                    "description": "Where to save the .hda file"
                },
                "version": {
                    "type": "string",
                    "description": "Optional version string"
                },
                "min_inputs": {
                    "type": "integer",
                    "description": "Minimum number of inputs"
                },
                "max_inputs": {
                    "type": "integer",
                    "description": "Maximum number of inputs"
                }
            },
            "required": ["node_path", "name", "label", "file_path"]
        }
    ),
    Tool(
        name="houdini_hda_install",
        description="Install an HDA file into the current Houdini session.",
        inputSchema={
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the .hda file to install"
                }
            },
            "required": ["file_path"]
        }
    ),
    Tool(
        name="houdini_hda_reload",
        description="Reload HDA definitions. Reloads a specific file or all HDA files if no path specified.",
        inputSchema={
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Optional path to a specific .hda file to reload. If omitted, reloads all HDA files."
                }
            }
        }
    ),
]


# =============================================================================
# VGGT Custom Handlers
# =============================================================================

# Module name → (type_name, category)
_VGGT_MODULES = {
    'Reconstruct': ('vggt::VGGT_Reconstruct::1.0', 'Sop'),
    'Depth Fields': ('vggt::VGGT_Depth_Fields::1.0', 'Sop'),
    'Mesher': ('vggt::VGGT_Mesher::1.0', 'Sop'),
    'Tracker': ('vggt::VGGT_Tracker::1.0', 'Sop'),
    'Texture Project': ('vggt::VGGT_Texture_Project::1.0', 'Sop'),
    'COLMAP Export': ('vggt::VGGT_COLMAP_Export::1.0', 'Driver'),
    'Dataset Export': ('vggt::VGGT_Dataset_Export::1.0', 'Driver'),
}

_VGGT_HDA_FILES = [
    'VGGT_Reconstruct.hda',
    'VGGT_Depth_Fields.hda',
    'VGGT_Mesher.hda',
    'VGGT_Tracker.hda',
    'VGGT_Texture_Project.hda',
    'VGGT_COLMAP_Export.hda',
    'VGGT_Dataset_Export.hda',
]


async def handle_vggt_setup(arguments: dict) -> list[ContentItem]:
    """Install all VGGT HDAs and check environment health."""
    vggt_root = os.environ.get('VGGT_ROOT')
    if not vggt_root:
        return error_result("VGGT_ROOT environment variable not set on MCP server")

    hda_dir = os.path.join(vggt_root, 'hda')
    if not os.path.isdir(hda_dir):
        return error_result(f"HDA directory not found: {hda_dir}")

    results = []
    errors = []

    for hda_file in _VGGT_HDA_FILES:
        hda_path = os.path.join(hda_dir, hda_file)
        if not os.path.exists(hda_path):
            errors.append(f"Missing: {hda_file}")
            continue

        try:
            result = await call_bridge('POST', '/hda/install', body={'file_path': hda_path})
            if isinstance(result, dict) and 'error' in result:
                errors.append(f"{hda_file}: {result['error']}")
            else:
                results.append({
                    'file': hda_file,
                    'definitions': result.get('definitions', []),
                })
        except Exception as e:
            errors.append(f"{hda_file}: {str(e)}")

    summary = {
        'installed': len(results),
        'errors': errors,
        'hda_dir': hda_dir,
        'hdas': results,
    }

    return [TextContent(type="text", text=format_result(summary))]


async def handle_vggt_create_node(arguments: dict) -> list[ContentItem]:
    """Create a VGGT node with correct container and initial parameters."""
    module = arguments.get('module')
    name = arguments.get('name')
    parms = arguments.get('parms', {})
    connect_to = arguments.get('connect_to')

    if module not in _VGGT_MODULES:
        return error_result(
            f"Unknown module: {module}. "
            f"Valid modules: {', '.join(_VGGT_MODULES.keys())}"
        )

    type_name, category = _VGGT_MODULES[module]

    try:
        if category == 'Sop':
            # SOPs need a geo container at /obj
            container_name = name or module.lower().replace(' ', '_')
            geo_result = await call_bridge('POST', '/node/create', body={
                'parent': '/obj',
                'type': 'geo',
                'name': f'vggt_{container_name}',
            })
            if isinstance(geo_result, dict) and 'error' in geo_result:
                return error_result(f"Failed to create geo container: {geo_result['error']}")

            geo_path = geo_result['path']

            # Create the VGGT SOP inside the geo
            sop_result = await call_bridge('POST', '/node/create', body={
                'parent': geo_path,
                'type': type_name,
                'name': name,
            })
            if isinstance(sop_result, dict) and 'error' in sop_result:
                return error_result(f"Failed to create VGGT node: {sop_result['error']}")

            node_path = sop_result['path']

            # Set display flag
            await call_bridge('POST', '/node/flag', body={
                'path': node_path,
                'flag': 'display',
                'value': True,
            })

        else:
            # Drivers go in /out
            sop_result = await call_bridge('POST', '/node/create', body={
                'parent': '/out',
                'type': type_name,
                'name': name,
            })
            if isinstance(sop_result, dict) and 'error' in sop_result:
                return error_result(f"Failed to create VGGT node: {sop_result['error']}")

            node_path = sop_result['path']
            geo_path = None

        # Set initial parameters
        for parm_name, value in parms.items():
            await call_bridge('POST', '/parm/set', body={
                'path': node_path,
                'parm': parm_name,
                'value': value,
            })

        # Connect input if specified
        if connect_to and category == 'Sop':
            await call_bridge('POST', '/node/connect', body={
                'from': connect_to,
                'to': node_path,
                'from_output': 0,
                'to_input': 0,
            })

        result = {
            'success': True,
            'module': module,
            'node_path': node_path,
            'type': type_name,
            'category': category,
        }
        if geo_path:
            result['container_path'] = geo_path

        return [TextContent(type="text", text=format_result(result))]

    except Exception as e:
        return error_result(f"Failed to create node: {str(e)}")


async def handle_vggt_execute(arguments: dict) -> list[ContentItem]:
    """Trigger on_execute on a VGGT node (GPU inference)."""
    path = arguments.get('path')
    timeout = min(arguments.get('timeout', 600), 3600)

    if not path:
        return error_result("Missing 'path' argument")

    try:
        # Use a custom long timeout for this call
        async with httpx.AsyncClient(timeout=float(timeout)) as client:
            url = f"{HOUDINI_BRIDGE_URL}/vggt/execute"
            response = await client.post(url, json={'path': path})
            response.raise_for_status()
            result = response.json()

        if isinstance(result, dict) and 'error' in result:
            return error_result(f"Execute failed: {result['error']}")

        return [TextContent(type="text", text=format_result(result))]

    except httpx.TimeoutException:
        return error_result(
            f"Execute timed out after {timeout}s. "
            "The job may still be running. Use houdini_vggt_pipeline_status to check."
        )
    except Exception as e:
        return error_result(f"Execute failed: {str(e)}")


async def handle_vggt_pipeline_status(arguments: dict) -> list[ContentItem]:
    """Get status of all VGGT nodes in the scene."""
    path = arguments.get('path')

    try:
        params = {}
        if path:
            params['path'] = path

        result = await call_bridge('GET', '/vggt/pipeline', params=params or None)

        if isinstance(result, dict) and 'error' in result:
            return error_result(result['error'])

        return [TextContent(type="text", text=format_result(result))]

    except Exception as e:
        return error_result(f"Pipeline status failed: {str(e)}")


async def handle_vggt_read_results(arguments: dict) -> list[ContentItem]:
    """Read artifacts from a completed VGGT node."""
    path = arguments.get('path')
    include = arguments.get('include', ['result', 'cameras', 'log', 'manifest', 'progress'])

    if not path:
        return error_result("Missing 'path' argument")

    try:
        params = {
            'path': path,
            'include': ','.join(include) if isinstance(include, list) else include,
        }

        result = await call_bridge('GET', '/vggt/results', params=params)

        if isinstance(result, dict) and 'error' in result:
            return error_result(result['error'])

        return [TextContent(type="text", text=format_result(result))]

    except Exception as e:
        return error_result(f"Read results failed: {str(e)}")


# Custom handler dispatch table
_VGGT_HANDLERS = {
    'houdini_vggt_setup': handle_vggt_setup,
    'houdini_vggt_create_node': handle_vggt_create_node,
    'houdini_vggt_execute': handle_vggt_execute,
    'houdini_vggt_pipeline_status': handle_vggt_pipeline_status,
    'houdini_vggt_read_results': handle_vggt_read_results,
}


# =============================================================================
# MCP Handlers
# =============================================================================

@server.list_tools()
async def list_tools() -> ListToolsResult:
    """List all available Houdini tools."""
    return ListToolsResult(tools=TOOLS)


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> CallToolResult:
    """Execute a Houdini tool."""
    logger.info(f"Calling tool: {name} with args: {arguments}")

    try:
        # Check for VGGT custom handlers first
        if name in _VGGT_HANDLERS:
            result = await _VGGT_HANDLERS[name](arguments)
            return CallToolResult(content=result)

        # Map tool names to bridge endpoints
        tool_map = {
            # GET requests
            'houdini_ping': ('GET', '/ping', None),
            'houdini_scene_info': ('GET', '/scene/info', None),
            'houdini_node_get': ('GET', '/node/get', {'path': arguments.get('path')}),
            'houdini_node_tree': ('GET', '/node/tree', {
                'root': arguments.get('root', '/obj'),
                'depth': arguments.get('depth', 2)
            }),
            'houdini_node_search': ('GET', '/node/search', {
                'pattern': arguments.get('pattern', '*'),
                'type': arguments.get('type'),
                'root': arguments.get('root', '/')
            }),
            'houdini_parm_get': ('GET', '/parm/get', {
                'path': arguments.get('path'),
                'parm': arguments.get('parm')
            }),
            'houdini_cook_status': ('GET', '/cook/status', None),
            'houdini_hda_list': ('GET', '/hda/list', {'category': arguments.get('category')}),

            # POST requests
            'houdini_node_create': ('POST', '/node/create', arguments),
            'houdini_node_delete': ('POST', '/node/delete', arguments),
            'houdini_node_rename': ('POST', '/node/rename', arguments),
            'houdini_node_connect': ('POST', '/node/connect', arguments),
            'houdini_node_disconnect': ('POST', '/node/disconnect', arguments),
            'houdini_node_flag': ('POST', '/node/flag', arguments),
            'houdini_node_layout': ('POST', '/node/layout', arguments),
            'houdini_parm_set': ('POST', '/parm/set', arguments),
            'houdini_parm_expression': ('POST', '/parm/expression', arguments),
            'houdini_parm_revert': ('POST', '/parm/revert', arguments),
            'houdini_frame_set': ('POST', '/frame/set', arguments),
            'houdini_scene_save': ('POST', '/scene/save', arguments),
            'houdini_geo_export': ('POST', '/geo/export', arguments),
            'houdini_render_snapshot': ('POST', '/render/snapshot', arguments),
            'houdini_render_flipbook': ('POST', '/render/flipbook', arguments),
            'houdini_batch': ('POST', '/batch', arguments),

            # PDG/TOPs
            'houdini_pdg_status': ('GET', '/pdg/status', {'path': arguments.get('path')}),
            'houdini_pdg_workitems': ('GET', '/pdg/workitems', {
                'path': arguments.get('path'),
                'state': arguments.get('state'),
            }),
            'houdini_pdg_cook': ('POST', '/pdg/cook', arguments),
            'houdini_pdg_dirty': ('POST', '/pdg/dirty', arguments),
            'houdini_pdg_cancel': ('POST', '/pdg/cancel', arguments),

            # USD/Solaris/LOPs
            'houdini_lop_stage_info': ('GET', '/lop/stage/info', {'path': arguments.get('path')}),
            'houdini_lop_prim_get': ('GET', '/lop/prim/get', {
                'path': arguments.get('path'),
                'prim_path': arguments.get('prim_path'),
                'include_attrs': arguments.get('include_attrs', True),
            }),
            'houdini_lop_layer_info': ('GET', '/lop/layer/info', {'path': arguments.get('path')}),
            'houdini_lop_prim_search': ('GET', '/lop/prim/search', {
                'path': arguments.get('path'),
                'pattern': arguments.get('pattern', '/**'),
                'type_name': arguments.get('type_name'),
            }),
            'houdini_lop_import': ('POST', '/lop/import', arguments),

            # Extraction
            'houdini_geo_info': ('GET', '/extract/geo_info', {
                'path': arguments.get('path'),
            }),
            'houdini_attrib_read': ('GET', '/extract/attrib_read', {
                'path': arguments.get('path'),
                'attrib_class': arguments.get('attrib_class', 'point'),
                'attrib_name': arguments.get('attrib_name'),
                'start': arguments.get('start', 0),
                'count': arguments.get('count', -1),
            }),
            'houdini_aov_list': ('GET', '/extract/aov_list', {
                'path': arguments.get('path'),
            }),
            'houdini_camera_get': ('GET', '/extract/camera_get', {
                'path': arguments.get('path'),
            }),

            # HDA Management
            'houdini_hda_get': ('GET', '/hda/get', {
                'node_type': arguments.get('node_type'),
                'category': arguments.get('category'),
            }),
            'houdini_hda_create': ('POST', '/hda/create', arguments),
            'houdini_hda_install': ('POST', '/hda/install', arguments),
            'houdini_hda_reload': ('POST', '/hda/reload', arguments),
        }

        if name not in tool_map:
            return CallToolResult(content=error_result(f"Unknown tool: {name}"))

        method, endpoint, data = tool_map[name]

        if method == 'GET':
            # Filter out None values from params
            params = {k: v for k, v in (data or {}).items() if v is not None}
            result = await call_bridge('GET', endpoint, params=params or None)
        else:
            result = await call_bridge('POST', endpoint, body=data)

        # Check for errors in response
        if isinstance(result, dict) and 'error' in result:
            return CallToolResult(content=[TextContent(
                type="text",
                text=f"Houdini error: {result['error']}"
            )])

        return CallToolResult(content=[TextContent(
            type="text",
            text=format_result(result)
        )])

    except httpx.ConnectError:
        return CallToolResult(content=error_result(
            "Cannot connect to Houdini. Make sure:\n"
            "1. Houdini is running\n"
            "2. The bridge server is started (run start_bridge() in Houdini)\n"
            f"3. Bridge is accessible at {HOUDINI_BRIDGE_URL}"
        ))

    except httpx.TimeoutException:
        return CallToolResult(content=error_result(
            f"Request timed out after {TIMEOUT}s. The operation may be taking too long."
        ))

    except httpx.HTTPStatusError as e:
        return CallToolResult(content=error_result(
            f"HTTP error {e.response.status_code}: {e.response.text}"
        ))

    except Exception as e:
        logger.exception(f"Error calling tool {name}")
        return CallToolResult(content=error_result(str(e)))


# =============================================================================
# Main Entry Point
# =============================================================================

async def main():
    """Run the MCP server with stdio transport."""
    from mcp.server.stdio import stdio_server

    logger.info(f"Starting Houdini MCP server (bridge: {HOUDINI_BRIDGE_URL})")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


async def main_http(host: str, port: int) -> None:
    """Run the MCP server with streamable HTTP transport."""
    import uvicorn
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.types import Receive, Scope, Send

    logger.info(
        f"Starting Houdini MCP HTTP server on {host}:{port} "
        f"(bridge: {HOUDINI_BRIDGE_URL})"
    )

    session_manager = StreamableHTTPSessionManager(app=server)

    class MCPTransportApp:
        """ASGI app that delegates to the session manager."""

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            await session_manager.handle_request(scope, receive, send)

    starlette_app = Starlette(
        routes=[Route("/mcp", endpoint=MCPTransportApp())],
        lifespan=lambda app: session_manager.run(),
    )

    config = uvicorn.Config(
        starlette_app,
        host=host,
        port=port,
        log_level="info",
    )
    uv_server = uvicorn.Server(config)
    await uv_server.serve()


def run():
    """Entry point for running as module."""
    parser = argparse.ArgumentParser(description="Houdini MCP Server")
    parser.add_argument(
        "--http", action="store_true",
        help="Use streamable HTTP transport instead of stdio",
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="HTTP server host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="HTTP server port (default: 8080)",
    )
    args = parser.parse_args()

    if args.http:
        asyncio.run(main_http(args.host, args.port))
    else:
        asyncio.run(main())


if __name__ == "__main__":
    run()
