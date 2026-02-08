# Houdini MCP Integration

Connect Claude agents (Claude Code, Claude.ai, VS Code) to Houdini 21+ via the Model Context Protocol.

## Overview

This integration enables Claude to:
- **Query** scene information, node trees, parameter values
- **Create** nodes, wire connections, build networks
- **Modify** parameters, expressions, flags
- **Export** geometry in various formats

The architecture consists of two components:
1. **Bridge Server** - Runs inside Houdini, exposes HTTP API
2. **MCP Server** - External process that Claude communicates with

```
┌─────────────┐     MCP      ┌─────────────┐     HTTP     ┌─────────────┐
│   Claude    │ ◄──────────► │  MCP Server │ ◄──────────► │   Houdini   │
│   Agent     │   (stdio)    │  (Python)   │  (localhost) │   Bridge    │
└─────────────┘              └─────────────┘              └─────────────┘
```

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/MoleskinProductions/houdini-mcp.git
cd houdini-mcp
```

### 2. Install Python dependencies

```bash
# Using pip
pip install -e .

# Or using uv (recommended)
uv pip install -e .
```

### 3. Set up the Houdini shelf tool

Option A: **Copy the bridge to Houdini's Python path**
```bash
# Copy to Houdini's packages directory
cp -r houdini_bridge ~/houdini21.0/python3.11libs/
```

Option B: **Add to PYTHONPATH in houdini.env**
```bash
# Add to ~/houdini21.0/houdini.env
PYTHONPATH = "/path/to/houdini-mcp:$PYTHONPATH"
```

Then create a shelf tool with this script:
```python
from houdini_bridge import toggle_bridge
toggle_bridge()
```

### 4. Configure Claude

**For Claude Code** (in your project directory):
```bash
cp .mcp.json /path/to/your/project/
```

**For Claude Desktop** (`~/.config/claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "houdini": {
      "command": "python",
      "args": ["-m", "houdini_mcp.server"],
      "env": {
        "HOUDINI_BRIDGE_URL": "http://127.0.0.1:8765"
      }
    }
  }
}
```

## Usage

### Starting the Bridge

1. Open Houdini
2. Click the MCP shelf tool (or run in Python shell):
   ```python
   from houdini_bridge import start_bridge
   start_bridge()
   ```
3. You should see: `[HoudiniBridge] Server started on http://127.0.0.1:8765`

### Talking to Houdini via Claude

Once the bridge is running and Claude has the MCP server configured:

```
You: Check if Houdini is connected

Claude: [uses houdini_ping tool]
Houdini 21.0.506 is running with the file "untitled.hip"

You: Create a sphere and scatter points on it

Claude: [uses houdini_node_create, houdini_node_create, houdini_node_connect tools]
Created a network with:
- /obj/geo1/sphere1 (sphere)
- /obj/geo1/scatter1 (scatter) connected to sphere

You: Set the scatter count to 5000

Claude: [uses houdini_parm_set tool]
Set npts = 5000 on /obj/geo1/scatter1
```

## Available Tools

### Read Operations
| Tool | Description |
|------|-------------|
| `houdini_ping` | Check connectivity and version |
| `houdini_scene_info` | Get scene info (hip file, frame range, etc.) |
| `houdini_node_get` | Get detailed node info |
| `houdini_node_tree` | Get hierarchical node tree |
| `houdini_node_search` | Search nodes by name/type |
| `houdini_parm_get` | Get parameter values |
| `houdini_parm_template` | Get parameter schema (names, types, defaults, ranges, menus) |
| `houdini_cook_status` | Cook/render status (state, progress, memory, errors, warnings) |
| `houdini_hda_list` | List available HDAs |

### Write Operations
| Tool | Description |
|------|-------------|
| `houdini_node_create` | Create a new node |
| `houdini_node_delete` | Delete a node |
| `houdini_node_rename` | Rename a node |
| `houdini_node_connect` | Connect two nodes |
| `houdini_node_disconnect` | Disconnect a node input |
| `houdini_node_flag` | Set display/render/bypass flags |
| `houdini_node_layout` | Auto-layout a network |
| `houdini_parm_set` | Set parameter value |
| `houdini_parm_expression` | Set parameter expression |
| `houdini_parm_revert` | Revert parameter to default |
| `houdini_frame_set` | Set current frame |
| `houdini_scene_save` | Save the scene |
| `houdini_geo_export` | Export geometry to file |
| `houdini_render_snapshot` | Capture viewport or Karma render snapshot |
| `houdini_render_flipbook` | Render a flipbook frame sequence |
| `houdini_batch` | Execute multiple operations atomically |

### PDG/TOPs Operations
| Tool | Description |
|------|-------------|
| `houdini_pdg_status` | Get graph cook state and work item counts |
| `houdini_pdg_workitems` | List work items with state, attributes, output files |
| `houdini_pdg_cook` | Start cooking a TOP graph (non-blocking) |
| `houdini_pdg_dirty` | Dirty/invalidate work items for re-cook |
| `houdini_pdg_cancel` | Cancel a running PDG cook |

### USD/Solaris/LOP Operations
| Tool | Description |
|------|-------------|
| `houdini_lop_stage_info` | Get USD stage info (prim count, layers, time codes) |
| `houdini_lop_prim_get` | Get detailed USD prim info |
| `houdini_lop_layer_info` | Get layer stack information |
| `houdini_lop_prim_search` | Search prims by path pattern and/or type |
| `houdini_lop_import` | Import USD file as reference or sublayer |

### HDA Management
| Tool | Description |
|------|-------------|
| `houdini_hda_get` | Get detailed HDA definition info |
| `houdini_hda_create` | Package a node into a reusable .hda file |
| `houdini_hda_install` | Install an HDA file into the session |
| `houdini_hda_reload` | Reload HDA definitions (specific file or all) |

### Data Extraction
| Tool | Description |
|------|-------------|
| `houdini_geo_info` | Geometry summary: counts, prim types, bounds, attributes, groups |
| `houdini_attrib_read` | Bulk-read attribute values (inline or file_ref for large data) |
| `houdini_aov_list` | AOV render pass configurations from LOP/ROP nodes |
| `houdini_camera_get` | Camera config: focal, aperture, resolution, world_matrix |

### VGGT Pipeline
| Tool | Description |
|------|-------------|
| `houdini_vggt_setup` | Install VGGT Toolkit HDAs and verify environment |
| `houdini_vggt_create_node` | Create a VGGT pipeline node with parameters |
| `houdini_vggt_execute` | Run GPU inference on a VGGT node |
| `houdini_vggt_pipeline_status` | Status of all VGGT nodes in the scene |
| `houdini_vggt_read_results` | Read output artifacts from a VGGT node |

## Auto-Start Bridge (Optional)

To automatically start the bridge when Houdini opens, add to `456.py`:

```python
# ~/houdini21.0/scripts/456.py
import hou
if hou.isUIAvailable():
    try:
        from houdini_bridge import start_bridge
        start_bridge()
    except ImportError:
        print("Houdini MCP bridge not installed")
```

## Troubleshooting

### "Cannot connect to Houdini"
- Make sure Houdini is running
- Make sure the bridge is started (check Houdini console for startup message)
- Verify port 8765 is not in use: `lsof -i :8765`

### "Node not found"
- Use full paths (e.g., `/obj/geo1/scatter1`, not just `scatter1`)
- Check the node exists: `houdini_node_tree` to see structure

### "Parameter not found"
- Check exact parameter name (use `houdini_parm_get` without `parm` arg to list all)
- For vector params, use the base name (e.g., `t` not `tx`)

### Bridge crashes
- Check Houdini console for Python errors
- Make sure you're using Houdini 21+ (Python 3.11)

## Development

### Running tests
```bash
pytest tests/
```

### Type checking
```bash
mypy houdini_mcp
```

### Linting
```bash
ruff check .
```

## Roadmap

- [x] **Phase 1**: Core bridge and read tools
- [x] **Phase 2**: Mutation tools (create, connect, set)
- [x] **Phase 3**: Geometry export
- [x] **Phase 4**: Render tools (viewport snapshot, Karma, flipbook)
- [x] **Phase 5**: PDG/TOPs, USD/Solaris/LOPs, HDA management
- [x] **Phase 5.5**: Data extraction plugin (geo_info, attrib_read, aov_list, camera_get)
- [x] **Phase 5.6**: VGGT pipeline tools (setup, create, execute, status, results)
- [ ] **Phase 6**: MCP Apps (interactive UI in Claude)
- [ ] **Phase 7**: Self-Generating UI (intent → interface synthesis)

## License

MIT
