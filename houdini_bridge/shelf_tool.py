"""
Houdini Shelf Tool Script

To install:
1. Create a new shelf in Houdini
2. Create a new tool with this script
3. Set the icon to MISC_python or any icon you prefer

The tool toggles the MCP bridge server on/off.
"""

import os
import sys

# Add the houdini-mcp directory to Python path
# Uses HOUDINI_MCP_PATH env var if set, otherwise resolves relative to this file
MCP_PATH = os.environ.get(
    "HOUDINI_MCP_PATH",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if MCP_PATH not in sys.path:
    sys.path.insert(0, MCP_PATH)

# Import and toggle
from houdini_bridge import is_running, toggle_bridge  # noqa: E402

toggle_bridge()

# Show status message
import hou  # noqa: E402

if is_running():
    hou.ui.displayMessage(
        "MCP Bridge started on http://127.0.0.1:8765\n\n"
        "Claude can now connect to Houdini.",
        title="Houdini MCP Bridge"
    )
else:
    hou.ui.displayMessage(
        "MCP Bridge stopped.",
        title="Houdini MCP Bridge"
    )
