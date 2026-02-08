"""
MCP tool mock tests for extraction endpoints.

Tests that the 4 new extraction tools correctly route through
the MCP server to the bridge extraction handlers.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


def _mock_bridge(method, endpoint, return_value):
    """Helper to create a patched call_bridge that asserts method + endpoint."""
    async def _side_effect(m, ep, params=None, body=None):
        assert m == method, f"Expected {method}, got {m}"
        assert ep == endpoint, f"Expected {endpoint}, got {ep}"
        return return_value
    return _side_effect


class TestGeoInfoTool:
    """Test houdini_geo_info MCP tool."""

    @pytest.mark.asyncio
    async def test_geo_info_routes_correctly(self):
        """houdini_geo_info should GET /extract/geo_info."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'node_path': '/obj/geo1/OUT',
            'point_count': 5000,
            'prim_count': 9800,
            'vertex_count': 39200,
            'prim_types': {'Poly': 9800},
            'bounds': {'min': [-0.5, -0.3, -0.4], 'max': [0.5, 0.8, 0.4]},
            'attributes': {
                'point': {'P': {'type': 'vector3', 'size': 3}},
                'primitive': {},
                'vertex': {},
                'detail': {},
            },
            'groups': {'point': [], 'prim': []},
            'memory_bytes': 1240000,
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/geo_info', mock_data)):
            result = await call_tool('houdini_geo_info', {'path': '/obj/geo1/OUT'})
            text = result.content[0].text
            assert '5000' in text
            assert 'point_count' in text

    @pytest.mark.asyncio
    async def test_geo_info_error_propagation(self):
        """Errors from bridge should be surfaced to the user."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'error': True,
            'code': 'NODE_NOT_FOUND',
            'message': 'No node exists at path /obj/missing',
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/geo_info', mock_data)):
            result = await call_tool('houdini_geo_info', {'path': '/obj/missing'})
            # The MCP server checks for 'error' key
            assert 'error' in result.content[0].text.lower()


class TestAttribReadTool:
    """Test houdini_attrib_read MCP tool."""

    @pytest.mark.asyncio
    async def test_attrib_read_inline(self):
        """houdini_attrib_read should GET /extract/attrib_read."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'node_path': '/obj/geo1/OUT',
            'attrib_class': 'point',
            'attrib_name': 'pscale',
            'type': 'float',
            'size': 1,
            'count': 3,
            'total': 3,
            'values': [1.0, 0.5, 0.8],
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/attrib_read', mock_data)):
            result = await call_tool('houdini_attrib_read', {
                'path': '/obj/geo1/OUT',
                'attrib_name': 'pscale',
            })
            text = result.content[0].text
            assert 'pscale' in text
            assert '1.0' in text

    @pytest.mark.asyncio
    async def test_attrib_read_file_ref(self):
        """Large attribute data should return a file_ref."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'type': 'file_ref',
            'path': '/tmp/pixel_vision/extract/attrib_P_abc123.bin',
            'metadata_path': '/tmp/pixel_vision/extract/attrib_P_abc123.json',
            'format': 'bin',
            'size_bytes': 2400000,
            'ttl_seconds': 300,
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/attrib_read', mock_data)):
            result = await call_tool('houdini_attrib_read', {
                'path': '/obj/geo1/OUT',
                'attrib_name': 'P',
            })
            text = result.content[0].text
            assert 'file_ref' in text
            assert 'attrib_P' in text


class TestAOVListTool:
    """Test houdini_aov_list MCP tool."""

    @pytest.mark.asyncio
    async def test_aov_list(self):
        """houdini_aov_list should GET /extract/aov_list."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'path': '/stage/karmarendersettings1',
            'aovs': [
                {
                    'aov_name': 'depth',
                    'aov_type': 'DEPTH',
                    'data_type': 'float',
                    'format': 'float32',
                    'source': 'builtin',
                    'lpe': None,
                    'normalize': True,
                    'normalize_range': [0.0, 100.0],
                },
                {
                    'aov_name': 'N_world',
                    'aov_type': 'NORMAL_WORLD',
                    'data_type': 'vector3f',
                    'format': 'float16',
                    'source': 'primvar',
                    'lpe': None,
                    'normalize': False,
                    'normalize_range': None,
                },
            ],
            'count': 2,
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/aov_list', mock_data)):
            result = await call_tool('houdini_aov_list', {'path': '/stage/karmarendersettings1'})
            text = result.content[0].text
            assert 'depth' in text
            assert 'N_world' in text
            assert 'DEPTH' in text


class TestCameraGetTool:
    """Test houdini_camera_get MCP tool."""

    @pytest.mark.asyncio
    async def test_camera_get(self):
        """houdini_camera_get should GET /extract/camera_get."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'path': '/obj/cam1',
            'resolution': [1920, 1080],
            'focal_length': 50.0,
            'aperture': 41.4214,
            'near_clip': 0.01,
            'far_clip': 10000.0,
            'transform': {
                'translate': [0.0, 1.0, -5.0],
                'rotate': [15.0, 0.0, 0.0],
                'world_matrix': [
                    [1, 0, 0, 0],
                    [0, 0.966, 0.259, 0],
                    [0, -0.259, 0.966, 0],
                    [0, 1, -5, 1],
                ],
            },
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/camera_get', mock_data)):
            result = await call_tool('houdini_camera_get', {'path': '/obj/cam1'})
            text = result.content[0].text
            assert 'cam1' in text
            assert '50.0' in text
            assert 'world_matrix' in text

    @pytest.mark.asyncio
    async def test_camera_get_4x4_matrix(self):
        """Camera world_matrix should be 4x4 row-major."""
        from houdini_mcp.server import call_tool

        mock_data = {
            'path': '/obj/cam1',
            'resolution': [1920, 1080],
            'focal_length': 50.0,
            'aperture': 41.4214,
            'near_clip': 0.01,
            'far_clip': 10000.0,
            'transform': {
                'translate': [0.0, 0.0, 0.0],
                'rotate': [0.0, 0.0, 0.0],
                'world_matrix': [
                    [1, 0, 0, 0],
                    [0, 1, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 0, 1],
                ],
            },
        }

        with patch('houdini_mcp.server.call_bridge',
                    side_effect=_mock_bridge('GET', '/extract/camera_get', mock_data)):
            result = await call_tool('houdini_camera_get', {'path': '/obj/cam1'})
            text = result.content[0].text
            # Verify 4x4 identity matrix is present (pretty-printed with newlines)
            assert 'world_matrix' in text
            # Check all four diagonal 1s appear
            import json
            parsed = json.loads(text)
            matrix = parsed['transform']['world_matrix']
            assert len(matrix) == 4
            assert all(len(row) == 4 for row in matrix)
            assert matrix[0][0] == 1 and matrix[1][1] == 1
            assert matrix[2][2] == 1 and matrix[3][3] == 1


class TestExtractionToolDefinitions:
    """Test that extraction tool definitions are properly registered."""

    def test_new_tools_exist_in_tools_list(self):
        from houdini_mcp.server import TOOLS

        tool_names = {t.name for t in TOOLS}
        assert 'houdini_geo_info' in tool_names
        assert 'houdini_attrib_read' in tool_names
        assert 'houdini_aov_list' in tool_names
        assert 'houdini_camera_get' in tool_names

    def test_new_tools_have_required_fields(self):
        from houdini_mcp.server import TOOLS

        extraction_tools = [t for t in TOOLS if t.name in (
            'houdini_geo_info', 'houdini_attrib_read',
            'houdini_aov_list', 'houdini_camera_get',
        )]

        assert len(extraction_tools) == 4

        for tool in extraction_tools:
            assert tool.description, f"Tool {tool.name} missing description"
            assert tool.inputSchema, f"Tool {tool.name} missing inputSchema"
            assert 'path' in tool.inputSchema.get('required', []), \
                f"Tool {tool.name} should require 'path'"

    def test_attrib_read_requires_attrib_name(self):
        from houdini_mcp.server import TOOLS

        attrib_tool = next(t for t in TOOLS if t.name == 'houdini_attrib_read')
        assert 'attrib_name' in attrib_tool.inputSchema.get('required', [])
