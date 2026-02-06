"""
Tests for the Houdini MCP integration.

These tests can be run without Houdini to verify MCP server structure.
For full integration tests, run with Houdini bridge active.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest


# Test MCP server tool definitions
class TestToolDefinitions:
    """Test that tool definitions are properly structured."""

    def test_tools_have_required_fields(self):
        """All tools should have name, description, and inputSchema."""
        from houdini_mcp.server import TOOLS

        for tool in TOOLS:
            assert tool.name, "Tool missing name"
            assert tool.description, f"Tool {tool.name} missing description"
            assert tool.inputSchema, f"Tool {tool.name} missing inputSchema"

    def test_tool_names_are_prefixed(self):
        """All tool names should start with 'houdini_'."""
        from houdini_mcp.server import TOOLS

        for tool in TOOLS:
            assert tool.name.startswith('houdini_'), \
                f"Tool {tool.name} should start with 'houdini_'"

    def test_required_params_are_defined(self):
        """Required parameters should exist in properties."""
        from houdini_mcp.server import TOOLS

        for tool in TOOLS:
            schema = tool.inputSchema
            required = schema.get('required', [])
            properties = schema.get('properties', {})

            for param in required:
                assert param in properties, \
                    f"Tool {tool.name}: required param '{param}' not in properties"


class TestBridgeCommunication:
    """Test bridge communication helpers."""

    @pytest.mark.asyncio
    async def test_call_bridge_formats_get_request(self):
        """GET requests should use query parameters."""
        from unittest.mock import MagicMock

        from houdini_mcp.server import call_bridge

        with patch('houdini_mcp.server.httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = {'status': 'ok'}
            mock_response.raise_for_status = MagicMock()

            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            result = await call_bridge('GET', '/ping', params={'test': 'value'})

            mock_instance.get.assert_called_once()
            assert result == {'status': 'ok'}

    @pytest.mark.asyncio
    async def test_call_bridge_formats_post_request(self):
        """POST requests should use JSON body."""
        from unittest.mock import MagicMock

        from houdini_mcp.server import call_bridge

        with patch('houdini_mcp.server.httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = {'success': True}
            mock_response.raise_for_status = MagicMock()

            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            result = await call_bridge('POST', '/node/create', body={'type': 'geo'})

            mock_instance.post.assert_called_once()
            assert result == {'success': True}


class TestFormatting:
    """Test result formatting."""

    def test_format_result_pretty_prints(self):
        """Results should be formatted as pretty JSON."""
        from houdini_mcp.server import format_result

        data = {'key': 'value', 'nested': {'a': 1}}
        result = format_result(data)

        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed == data

        # Should be indented (not compact)
        assert '\n' in result

    def test_error_result_format(self):
        """Error results should have proper structure."""
        from houdini_mcp.server import error_result

        result = error_result("Test error")

        assert len(result) == 1
        assert result[0].type == "text"
        assert "ERROR:" in result[0].text
        assert "Test error" in result[0].text


# =============================================================================
# Mock-based tool endpoint tests
# =============================================================================

def _mock_bridge(method, endpoint, return_value):
    """Helper to create a patched call_bridge that asserts method + endpoint."""
    async def _side_effect(m, ep, params=None, body=None):
        assert m == method, f"Expected {method}, got {m}"
        assert ep == endpoint, f"Expected {endpoint}, got {ep}"
        return return_value
    return _side_effect


class TestPDGTools:
    """Test PDG/TOPs tool endpoints."""

    @pytest.mark.asyncio
    async def test_pdg_status(self):
        """houdini_pdg_status should GET /pdg/status."""
        from houdini_mcp.server import call_tool
        mock_data = {'state': 'idle', 'total': 10, 'cooked': 10}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/pdg/status', mock_data)):
            result = await call_tool('houdini_pdg_status', {'path': '/tasks/topnet1'})
            assert 'idle' in result.content[0].text

    @pytest.mark.asyncio
    async def test_pdg_workitems(self):
        """houdini_pdg_workitems should GET /pdg/workitems."""
        from houdini_mcp.server import call_tool
        mock_data = {'items': [{'name': 'item0', 'state': 'cooked'}]}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/pdg/workitems', mock_data)):
            result = await call_tool('houdini_pdg_workitems', {'path': '/tasks/topnet1/rop1'})
            assert 'item0' in result.content[0].text

    @pytest.mark.asyncio
    async def test_pdg_cook(self):
        """houdini_pdg_cook should POST /pdg/cook."""
        from houdini_mcp.server import call_tool
        mock_data = {'status': 'cooking_started'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/pdg/cook', mock_data)):
            result = await call_tool('houdini_pdg_cook', {'path': '/tasks/topnet1'})
            assert 'cooking_started' in result.content[0].text

    @pytest.mark.asyncio
    async def test_pdg_dirty(self):
        """houdini_pdg_dirty should POST /pdg/dirty."""
        from houdini_mcp.server import call_tool
        mock_data = {'dirtied': 5}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/pdg/dirty', mock_data)):
            result = await call_tool('houdini_pdg_dirty', {'path': '/tasks/topnet1', 'dirty_all': True})
            assert '5' in result.content[0].text

    @pytest.mark.asyncio
    async def test_pdg_cancel(self):
        """houdini_pdg_cancel should POST /pdg/cancel."""
        from houdini_mcp.server import call_tool
        mock_data = {'cancelled': True}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/pdg/cancel', mock_data)):
            result = await call_tool('houdini_pdg_cancel', {'path': '/tasks/topnet1'})
            assert 'true' in result.content[0].text.lower()


class TestLOPTools:
    """Test USD/Solaris/LOP tool endpoints."""

    @pytest.mark.asyncio
    async def test_lop_stage_info(self):
        """houdini_lop_stage_info should GET /lop/stage/info."""
        from houdini_mcp.server import call_tool
        mock_data = {'prim_count': 42, 'default_prim': '/world'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/lop/stage/info', mock_data)):
            result = await call_tool('houdini_lop_stage_info', {'path': '/stage/sublayer1'})
            assert '42' in result.content[0].text

    @pytest.mark.asyncio
    async def test_lop_prim_get(self):
        """houdini_lop_prim_get should GET /lop/prim/get."""
        from houdini_mcp.server import call_tool
        mock_data = {'type': 'Mesh', 'path': '/world/geo', 'active': True}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/lop/prim/get', mock_data)):
            result = await call_tool('houdini_lop_prim_get', {'path': '/stage/sublayer1', 'prim_path': '/world/geo'})
            assert 'Mesh' in result.content[0].text

    @pytest.mark.asyncio
    async def test_lop_layer_info(self):
        """houdini_lop_layer_info should GET /lop/layer/info."""
        from houdini_mcp.server import call_tool
        mock_data = {'layers': [{'identifier': 'anon:0x1234'}]}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/lop/layer/info', mock_data)):
            result = await call_tool('houdini_lop_layer_info', {'path': '/stage/sublayer1'})
            assert 'anon:0x1234' in result.content[0].text

    @pytest.mark.asyncio
    async def test_lop_prim_search(self):
        """houdini_lop_prim_search should GET /lop/prim/search."""
        from houdini_mcp.server import call_tool
        mock_data = {'prims': ['/world/geo', '/world/light']}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/lop/prim/search', mock_data)):
            result = await call_tool('houdini_lop_prim_search', {'path': '/stage/sublayer1', 'pattern': '/world/**'})
            assert '/world/geo' in result.content[0].text

    @pytest.mark.asyncio
    async def test_lop_import(self):
        """houdini_lop_import should POST /lop/import."""
        from houdini_mcp.server import call_tool
        mock_data = {'created_node': '/stage/reference1'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/lop/import', mock_data)):
            result = await call_tool('houdini_lop_import', {'path': '/stage', 'file': '/tmp/scene.usd'})
            assert 'reference1' in result.content[0].text


class TestHDATools:
    """Test HDA management tool endpoints."""

    @pytest.mark.asyncio
    async def test_hda_get(self):
        """houdini_hda_get should GET /hda/get."""
        from houdini_mcp.server import call_tool
        mock_data = {'name': 'my_hda', 'version': '1.0', 'inputs': 2}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/hda/get', mock_data)):
            result = await call_tool('houdini_hda_get', {'node_type': 'my_hda'})
            assert 'my_hda' in result.content[0].text

    @pytest.mark.asyncio
    async def test_hda_create(self):
        """houdini_hda_create should POST /hda/create."""
        from houdini_mcp.server import call_tool
        mock_data = {'file': '/tmp/my_hda.hda', 'success': True}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/hda/create', mock_data)):
            result = await call_tool('houdini_hda_create', {
                'node_path': '/obj/geo1',
                'name': 'my_hda',
                'label': 'My HDA',
                'file_path': '/tmp/my_hda.hda',
            })
            assert 'my_hda' in result.content[0].text

    @pytest.mark.asyncio
    async def test_hda_install(self):
        """houdini_hda_install should POST /hda/install."""
        from houdini_mcp.server import call_tool
        mock_data = {'installed': True, 'file': '/tmp/my_hda.hda'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/hda/install', mock_data)):
            result = await call_tool('houdini_hda_install', {'file_path': '/tmp/my_hda.hda'})
            assert 'true' in result.content[0].text.lower()

    @pytest.mark.asyncio
    async def test_hda_reload(self):
        """houdini_hda_reload should POST /hda/reload."""
        from houdini_mcp.server import call_tool
        mock_data = {'reloaded': True}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/hda/reload', mock_data)):
            result = await call_tool('houdini_hda_reload', {})
            assert 'true' in result.content[0].text.lower()


class TestBatchAndRender:
    """Test batch and render tool endpoints."""

    @pytest.mark.asyncio
    async def test_batch(self):
        """houdini_batch should POST /batch."""
        from houdini_mcp.server import call_tool
        mock_data = {'results': [{'created': '/obj/geo1'}, {'connected': True}]}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/batch', mock_data)):
            result = await call_tool('houdini_batch', {
                'operations': [
                    {'type': 'create', 'args': {'parent': '/obj', 'type': 'geo'}},
                    {'type': 'connect', 'args': {'from': '/obj/geo1/a', 'to': '/obj/geo1/b'}},
                ]
            })
            assert '/obj/geo1' in result.content[0].text

    @pytest.mark.asyncio
    async def test_render_snapshot(self):
        """houdini_render_snapshot should POST /render/snapshot."""
        from houdini_mcp.server import call_tool
        mock_data = {'output': '/tmp/snapshot.png', 'resolution': [1920, 1080]}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/render/snapshot', mock_data)):
            result = await call_tool('houdini_render_snapshot', {'type': 'viewport'})
            assert 'snapshot.png' in result.content[0].text

    @pytest.mark.asyncio
    async def test_render_flipbook(self):
        """houdini_render_flipbook should POST /render/flipbook."""
        from houdini_mcp.server import call_tool
        mock_data = {'output': '/tmp/flip.$F4.png', 'frames': [1, 100]}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/render/flipbook', mock_data)):
            result = await call_tool('houdini_render_flipbook', {'frame_range': [1, 100]})
            assert 'flip' in result.content[0].text


# Integration tests (require running Houdini bridge)
@pytest.mark.integration
class TestIntegration:
    """Integration tests that require a running Houdini bridge."""

    @pytest.fixture
    def bridge_url(self):
        return "http://127.0.0.1:8765"

    @pytest.mark.asyncio
    async def test_ping(self, bridge_url):
        """Test ping endpoint."""
        import httpx

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{bridge_url}/ping")
                data = response.json()
                assert data['status'] == 'ok'
                assert 'houdini_version' in data
            except httpx.ConnectError:
                pytest.skip("Houdini bridge not running")

    @pytest.mark.asyncio
    async def test_scene_info(self, bridge_url):
        """Test scene info endpoint."""
        import httpx

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{bridge_url}/scene/info")
                data = response.json()
                assert 'hip_file' in data
                assert 'fps' in data
                assert 'contexts' in data
            except httpx.ConnectError:
                pytest.skip("Houdini bridge not running")

    @pytest.mark.asyncio
    async def test_node_tree(self, bridge_url):
        """Test node tree endpoint."""
        import httpx

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{bridge_url}/node/tree",
                    params={'root': '/obj', 'depth': 1}
                )
                data = response.json()
                assert 'path' in data
                assert data['path'] == '/obj'
            except httpx.ConnectError:
                pytest.skip("Houdini bridge not running")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
