"""
Houdini Shelf Tool Script

To install:
1. Create a new shelf in Houdini
2. Create a new tool with this script
3. Set the icon to MISC_python or any icon you prefer

The tool toggles the MCP bridge server on/off.
"""

import sys
import os

# Add the houdini-mcp directory to Python path
# Adjust this path to where you installed the package
MCP_PATH = os.path.expanduser("~/houdini-mcp")  # or wherever you cloned it
if MCP_PATH not in sys.path:
    sys.path.insert(0, MCP_PATH)

# Import and toggle
from houdini_bridge import toggle_bridge, is_running

toggle_bridge()

# Show status message
import hou
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
