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


# =============================================================================
# Node & Parameter mutation tool tests
# =============================================================================

class TestNodeMutationTools:
    """Test node creation, deletion, connection, and flag tools."""

    @pytest.mark.asyncio
    async def test_node_create(self):
        """houdini_node_create should POST /node/create."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'path': '/obj/geo1/scatter1', 'name': 'scatter1', 'type': 'scatter'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/node/create', mock_data)):
            result = await call_tool('houdini_node_create', {'parent': '/obj/geo1', 'type': 'scatter'})
            assert 'scatter1' in result.content[0].text

    @pytest.mark.asyncio
    async def test_node_delete(self):
        """houdini_node_delete should POST /node/delete."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'deleted': '/obj/geo1/scatter1'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/node/delete', mock_data)):
            result = await call_tool('houdini_node_delete', {'path': '/obj/geo1/scatter1'})
            assert 'success' in result.content[0].text

    @pytest.mark.asyncio
    async def test_node_connect(self):
        """houdini_node_connect should POST /node/connect."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'from': '/obj/geo1/grid1', 'to': '/obj/geo1/scatter1'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/node/connect', mock_data)):
            result = await call_tool('houdini_node_connect', {
                'from': '/obj/geo1/grid1', 'to': '/obj/geo1/scatter1'
            })
            assert 'success' in result.content[0].text

    @pytest.mark.asyncio
    async def test_node_flag(self):
        """houdini_node_flag should POST /node/flag."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'path': '/obj/geo1/scatter1', 'flag': 'display', 'value': True}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/node/flag', mock_data)):
            result = await call_tool('houdini_node_flag', {
                'path': '/obj/geo1/scatter1', 'flag': 'display', 'value': True
            })
            assert 'display' in result.content[0].text

    @pytest.mark.asyncio
    async def test_node_create_error(self):
        """Node create with contract error should report the error."""
        from houdini_mcp.server import call_tool
        mock_data = {'error': True, 'code': 'NODE_NOT_FOUND', 'message': 'Parent not found: /obj/missing'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/node/create', mock_data)):
            result = await call_tool('houdini_node_create', {'parent': '/obj/missing', 'type': 'grid'})
            text = result.content[0].text
            assert 'NODE_NOT_FOUND' in text
            assert 'error' in text.lower()


class TestParmMutationTools:
    """Test parameter set and revert tools."""

    @pytest.mark.asyncio
    async def test_parm_set(self):
        """houdini_parm_set should POST /parm/set."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'parm': 'tx', 'value': 5.0}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/parm/set', mock_data)):
            result = await call_tool('houdini_parm_set', {
                'path': '/obj/geo1', 'parm': 'tx', 'value': 5.0
            })
            assert '5.0' in result.content[0].text

    @pytest.mark.asyncio
    async def test_parm_revert(self):
        """houdini_parm_revert should POST /parm/revert."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'parm': 'tx', 'value': 0.0}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/parm/revert', mock_data)):
            result = await call_tool('houdini_parm_revert', {'path': '/obj/geo1', 'parm': 'tx'})
            assert 'success' in result.content[0].text


class TestSceneTools:
    """Test scene-level mutation tools."""

    @pytest.mark.asyncio
    async def test_scene_save(self):
        """houdini_scene_save should POST /scene/save."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'path': '/tmp/test.hip'}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/scene/save', mock_data)):
            result = await call_tool('houdini_scene_save', {})
            assert 'test.hip' in result.content[0].text

    @pytest.mark.asyncio
    async def test_frame_set(self):
        """houdini_frame_set should POST /frame/set."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'frame': 24}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/frame/set', mock_data)):
            result = await call_tool('houdini_frame_set', {'frame': 24})
            assert '24' in result.content[0].text

    @pytest.mark.asyncio
    async def test_geo_export(self):
        """houdini_geo_export should POST /geo/export."""
        from houdini_mcp.server import call_tool
        mock_data = {'success': True, 'output': '/tmp/mesh.obj', 'format': 'obj', 'stats': {'points': 100}}
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('POST', '/geo/export', mock_data)):
            result = await call_tool('houdini_geo_export', {'path': '/obj/geo1/OUT', 'format': 'obj'})
            assert 'mesh.obj' in result.content[0].text


# =============================================================================
# VGGT Tool tests
# =============================================================================

class TestVGGTToolDefinitions:
    """Test that VGGT tool definitions are properly structured."""

    def test_vggt_tools_exist(self):
        """All 5 VGGT tools should be defined."""
        from houdini_mcp.server import TOOLS

        vggt_names = {t.name for t in TOOLS if 'vggt' in t.name}
        expected = {
            'houdini_vggt_setup',
            'houdini_vggt_create_node',
            'houdini_vggt_execute',
            'houdini_vggt_pipeline_status',
            'houdini_vggt_read_results',
        }
        assert vggt_names == expected

    def test_vggt_create_node_has_module_enum(self):
        """houdini_vggt_create_node should have module enum with all 7 modules."""
        from houdini_mcp.server import TOOLS

        tool = next(t for t in TOOLS if t.name == 'houdini_vggt_create_node')
        module_prop = tool.inputSchema['properties']['module']
        assert 'enum' in module_prop
        assert len(module_prop['enum']) == 7
        assert 'Reconstruct' in module_prop['enum']
        assert 'Dataset Export' in module_prop['enum']

    def test_vggt_execute_has_timeout(self):
        """houdini_vggt_execute should have a timeout parameter."""
        from houdini_mcp.server import TOOLS

        tool = next(t for t in TOOLS if t.name == 'houdini_vggt_execute')
        assert 'timeout' in tool.inputSchema['properties']

    def test_vggt_read_results_has_include(self):
        """houdini_vggt_read_results should have an include parameter."""
        from houdini_mcp.server import TOOLS

        tool = next(t for t in TOOLS if t.name == 'houdini_vggt_read_results')
        assert 'include' in tool.inputSchema['properties']


class TestVGGTSetup:
    """Test houdini_vggt_setup handler."""

    @pytest.mark.asyncio
    async def test_setup_missing_vggt_root(self):
        """Should error when VGGT_ROOT is not set."""
        from houdini_mcp.server import call_tool

        with patch.dict('os.environ', {}, clear=True):
            # Remove VGGT_ROOT if present
            import os
            env = os.environ.copy()
            env.pop('VGGT_ROOT', None)
            with patch.dict('os.environ', env, clear=True):
                result = await call_tool('houdini_vggt_setup', {})
                assert 'ERROR' in result.content[0].text or 'VGGT_ROOT' in result.content[0].text

    @pytest.mark.asyncio
    async def test_setup_installs_hdas(self, tmp_path):
        """Should install all HDA files found in the hda directory."""
        from houdini_mcp.server import call_tool

        # Create fake HDA files
        hda_dir = tmp_path / 'hda'
        hda_dir.mkdir()
        for name in ['VGGT_Reconstruct.hda', 'VGGT_Depth_Fields.hda',
                      'VGGT_Mesher.hda', 'VGGT_Tracker.hda',
                      'VGGT_Texture_Project.hda', 'VGGT_COLMAP_Export.hda',
                      'VGGT_Dataset_Export.hda']:
            (hda_dir / name).touch()

        mock_install = AsyncMock(return_value={'success': True, 'definitions': [{'name': 'test'}]})

        with patch.dict('os.environ', {'VGGT_ROOT': str(tmp_path)}):
            with patch('houdini_mcp.server.call_bridge', mock_install):
                result = await call_tool('houdini_vggt_setup', {})
                text = result.content[0].text
                parsed = json.loads(text)
                assert parsed['installed'] == 7
                assert len(parsed['errors']) == 0


class TestVGGTCreateNode:
    """Test houdini_vggt_create_node handler."""

    @pytest.mark.asyncio
    async def test_create_sop_node(self):
        """SOP modules should create a geo container + VGGT node inside."""
        from houdini_mcp.server import call_tool

        call_count = 0

        async def mock_bridge(method, endpoint, params=None, body=None):
            nonlocal call_count
            call_count += 1
            if endpoint == '/node/create' and body.get('type') == 'geo':
                return {'success': True, 'path': '/obj/vggt_reconstruct', 'name': 'vggt_reconstruct', 'type': 'geo'}
            elif endpoint == '/node/create':
                return {'success': True, 'path': '/obj/vggt_reconstruct/VGGT_Reconstruct1', 'name': 'VGGT_Reconstruct1', 'type': 'vggt::VGGT_Reconstruct::1.0'}
            elif endpoint == '/node/flag':
                return {'success': True}
            return {'success': True}

        with patch('houdini_mcp.server.call_bridge', side_effect=mock_bridge):
            result = await call_tool('houdini_vggt_create_node', {'module': 'Reconstruct'})
            text = result.content[0].text
            parsed = json.loads(text)
            assert parsed['success'] is True
            assert 'Reconstruct' in parsed['node_path'] or 'vggt' in parsed['node_path']
            assert parsed['category'] == 'Sop'

    @pytest.mark.asyncio
    async def test_create_driver_node(self):
        """Driver modules should be created directly in /out."""
        from houdini_mcp.server import call_tool

        async def mock_bridge(method, endpoint, params=None, body=None):
            if endpoint == '/node/create':
                return {'success': True, 'path': '/out/VGGT_COLMAP_Export1', 'name': 'VGGT_COLMAP_Export1', 'type': 'vggt::VGGT_COLMAP_Export::1.0'}
            return {'success': True}

        with patch('houdini_mcp.server.call_bridge', side_effect=mock_bridge):
            result = await call_tool('houdini_vggt_create_node', {'module': 'COLMAP Export'})
            parsed = json.loads(result.content[0].text)
            assert parsed['success'] is True
            assert parsed['category'] == 'Driver'

    @pytest.mark.asyncio
    async def test_create_unknown_module(self):
        """Unknown module should return error."""
        from houdini_mcp.server import call_tool

        result = await call_tool('houdini_vggt_create_node', {'module': 'NotAModule'})
        assert 'ERROR' in result.content[0].text

    @pytest.mark.asyncio
    async def test_create_with_parms(self):
        """Should set initial parameters after creation."""
        from houdini_mcp.server import call_tool

        parm_calls = []

        async def mock_bridge(method, endpoint, params=None, body=None):
            if endpoint == '/node/create' and body.get('type') == 'geo':
                return {'success': True, 'path': '/obj/vggt_reconstruct', 'name': 'vggt_reconstruct', 'type': 'geo'}
            elif endpoint == '/node/create':
                return {'success': True, 'path': '/obj/vggt_reconstruct/VGGT_Reconstruct1', 'name': 'VGGT_Reconstruct1', 'type': 'vggt::VGGT_Reconstruct::1.0'}
            elif endpoint == '/parm/set':
                parm_calls.append(body)
                return {'success': True}
            elif endpoint == '/node/flag':
                return {'success': True}
            return {'success': True}

        with patch('houdini_mcp.server.call_bridge', side_effect=mock_bridge):
            await call_tool('houdini_vggt_create_node', {
                'module': 'Reconstruct',
                'parms': {'image_dir': '/tmp/images', 'resize_long_edge': 512},
            })
            assert len(parm_calls) == 2


class TestVGGTExecute:
    """Test houdini_vggt_execute handler."""

    @pytest.mark.asyncio
    async def test_execute_success(self):
        """Should POST to /vggt/execute and return result."""
        from unittest.mock import MagicMock

        from houdini_mcp.server import call_tool

        mock_response = MagicMock()
        mock_response.json.return_value = {
            'success': True, 'path': '/obj/geo1/recon1',
            'result': {'status': 'ok', 'stats': {'num_cameras': 10}}
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None

        with patch('houdini_mcp.server.httpx.AsyncClient', return_value=mock_client):
            result = await call_tool('houdini_vggt_execute', {
                'path': '/obj/geo1/recon1',
                'timeout': 120,
            })
            text = result.content[0].text
            assert 'num_cameras' in text

    @pytest.mark.asyncio
    async def test_execute_missing_path(self):
        """Should error when path is missing."""
        from houdini_mcp.server import call_tool

        result = await call_tool('houdini_vggt_execute', {})
        assert 'ERROR' in result.content[0].text

    @pytest.mark.asyncio
    async def test_execute_timeout_capped(self):
        """Timeout should be capped at 3600."""
        from unittest.mock import MagicMock

        from houdini_mcp.server import call_tool

        mock_response = MagicMock()
        mock_response.json.return_value = {'success': True, 'path': '/obj/geo1/r'}
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None

        with patch('houdini_mcp.server.httpx.AsyncClient', return_value=mock_client) as mock_cls:
            await call_tool('houdini_vggt_execute', {
                'path': '/obj/geo1/r', 'timeout': 9999
            })
            # Verify timeout was capped to 3600
            mock_cls.assert_called_with(timeout=3600.0)


class TestVGGTPipelineStatus:
    """Test houdini_vggt_pipeline_status handler."""

    @pytest.mark.asyncio
    async def test_pipeline_status(self):
        """Should GET /vggt/pipeline and return node list."""
        from houdini_mcp.server import call_tool
        mock_data = {
            'nodes': [
                {'path': '/obj/geo1/recon1', 'type': 'vggt::VGGT_Reconstruct::1.0', 'status': 'done'},
            ],
            'count': 1,
        }
        with patch('houdini_mcp.server.call_bridge', side_effect=_mock_bridge('GET', '/vggt/pipeline', mock_data)):
            result = await call_tool('houdini_vggt_pipeline_status', {})
            text = result.content[0].text
            assert 'recon1' in text
            assert 'done' in text

    @pytest.mark.asyncio
    async def test_pipeline_status_with_filter(self):
        """Should pass path filter to bridge."""
        from houdini_mcp.server import call_tool

        async def mock_bridge_fn(method, endpoint, params=None, body=None):
            assert method == 'GET'
            assert endpoint == '/vggt/pipeline'
            assert params and params.get('path') == '/obj/geo1/recon1'
            return {'nodes': [], 'count': 0}

        with patch('houdini_mcp.server.call_bridge', side_effect=mock_bridge_fn):
            result = await call_tool('houdini_vggt_pipeline_status', {'path': '/obj/geo1/recon1'})
            parsed = json.loads(result.content[0].text)
            assert parsed['count'] == 0


class TestVGGTReadResults:
    """Test houdini_vggt_read_results handler."""

    @pytest.mark.asyncio
    async def test_read_results(self):
        """Should GET /vggt/results with include params."""
        from houdini_mcp.server import call_tool
        mock_data = {
            'path': '/obj/geo1/recon1',
            'result_dir': '/tmp/cache/recon1',
            'result': {'status': 'ok', 'stats': {'num_cameras': 10}},
            'cameras': {'cameras': [{'image': '001.png'}]},
            'files': [{'name': 'result.json', 'size': 256}],
        }

        async def mock_bridge_fn(method, endpoint, params=None, body=None):
            assert method == 'GET'
            assert endpoint == '/vggt/results'
            assert params['path'] == '/obj/geo1/recon1'
            return mock_data

        with patch('houdini_mcp.server.call_bridge', side_effect=mock_bridge_fn):
            result = await call_tool('houdini_vggt_read_results', {
                'path': '/obj/geo1/recon1',
                'include': ['result', 'cameras'],
            })
            text = result.content[0].text
            assert 'num_cameras' in text
            assert '001.png' in text

    @pytest.mark.asyncio
    async def test_read_results_missing_path(self):
        """Should error when path is missing."""
        from houdini_mcp.server import call_tool

        result = await call_tool('houdini_vggt_read_results', {})
        assert 'ERROR' in result.content[0].text


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
