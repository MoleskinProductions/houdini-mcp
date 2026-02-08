"""
Unit tests for houdini_extraction serializers, file_ref, and invalidation.

These tests run without Houdini by mocking hou.* objects.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest


# =============================================================================
# Serializer tests
# =============================================================================

class TestClassifyParmType:
    """Test _classify_parm_type mapping."""

    def _make_template(self, type_name, num_components=1, naming_scheme='Base'):
        tmpl = MagicMock()
        tmpl.type().name.return_value = type_name
        tmpl.numComponents.return_value = num_components
        scheme_mock = MagicMock()
        scheme_mock.name.return_value = naming_scheme
        tmpl.namingScheme.return_value = scheme_mock
        return tmpl

    def test_float_scalar(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 1)) == 'float'

    def test_float_vector2(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 2)) == 'vector2'

    def test_float_vector3(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 3)) == 'vector3'

    def test_float_color3(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 3, 'RGBA')) == 'color'

    def test_float_vector4(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 4)) == 'vector4'

    def test_float_color_alpha(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Float', 4, 'RGBA')) == 'color_alpha'

    def test_int_scalar(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Int', 1)) == 'int'

    def test_int_vector3(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Int', 3)) == 'vector3'

    def test_string(self):
        from houdini_extraction.serializers import _classify_parm_type
        tmpl = self._make_template('String', 1)
        tmpl.stringType().name.return_value = 'Regular'
        assert _classify_parm_type(tmpl) == 'string'

    def test_string_file(self):
        from houdini_extraction.serializers import _classify_parm_type
        tmpl = self._make_template('String', 1)
        tmpl.stringType().name.return_value = 'FileReference'
        assert _classify_parm_type(tmpl) == 'file'

    def test_toggle(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Toggle', 1)) == 'toggle'

    def test_menu(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Menu', 1)) == 'menu'

    def test_button(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Button', 1)) == 'button'

    def test_ramp(self):
        from houdini_extraction.serializers import _classify_parm_type
        assert _classify_parm_type(self._make_template('Ramp', 1)) == 'ramp'


class TestClassifyAttribType:
    """Test classify_attrib_type mapping."""

    def _make_attrib(self, data_type, size, name='test'):
        attrib = MagicMock()
        attrib.dataType().name.return_value = data_type
        attrib.size.return_value = size
        attrib.name.return_value = name
        return attrib

    def test_float_scalar(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('Float', 1)) == ('float', 1)

    def test_float_vector3(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('Float', 3, 'N')) == ('vector3', 3)

    def test_float_color(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('Float', 3, 'Cd')) == ('color', 3)

    def test_float_vector4(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('Float', 4, 'orient')) == ('vector4', 4)

    def test_int_scalar(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('Int', 1)) == ('int', 1)

    def test_string(self):
        from houdini_extraction.serializers import classify_attrib_type
        assert classify_attrib_type(self._make_attrib('String', 1)) == ('string', 1)


class TestSerializeNodeContract:
    """Test serialize_node_contract against §2.1 schema."""

    def _make_node(self):
        node = MagicMock()
        node.path.return_value = '/obj/geo1/scatter1'
        node.name.return_value = 'scatter1'
        node.type().name.return_value = 'scatter'
        node.type().category().name.return_value = 'Sop'
        node.parent().path.return_value = '/obj/geo1'

        # Inputs (one connected)
        input_node = MagicMock()
        input_node.path.return_value = '/obj/geo1/grid1'
        node.inputs.return_value = [input_node]

        # Outputs (one connected)
        output_node = MagicMock()
        output_node.path.return_value = '/obj/geo1/normal1'
        node.outputs.return_value = [output_node]

        # Children
        node.children.return_value = []

        # Flags
        node.isDisplayFlagSet.return_value = True
        node.isRenderFlagSet.return_value = False
        node.isBypassed.return_value = False
        node.isTemplateFlagSet.return_value = False

        # Errors/warnings
        node.errors.return_value = []
        node.warnings.return_value = []

        # Time dependence
        node.isTimeDependent.return_value = False

        return node

    def test_schema_fields(self):
        from houdini_extraction.serializers import serialize_node_contract
        node = self._make_node()
        result = serialize_node_contract(node)

        # Required §2.1 fields
        assert result['path'] == '/obj/geo1/scatter1'
        assert result['type'] == 'scatter'
        assert result['category'] == 'Sop'
        assert result['name'] == 'scatter1'
        assert result['parent'] == '/obj/geo1'
        assert result['children'] == []
        assert result['inputs'] == ['/obj/geo1/grid1']
        assert result['outputs'] == ['/obj/geo1/normal1']
        assert result['flags'] == {
            'display': True, 'render': False, 'bypass': False, 'template': False
        }
        assert result['errors'] == []
        assert result['warnings'] == []
        assert result['is_time_dependent'] is False

    def test_inputs_are_paths_not_null(self):
        """§2.1: inputs are ordered arrays of absolute paths, never null."""
        from houdini_extraction.serializers import serialize_node_contract
        node = self._make_node()
        node.inputs.return_value = []
        result = serialize_node_contract(node)
        assert result['inputs'] == []
        assert isinstance(result['inputs'], list)


# =============================================================================
# File reference tests
# =============================================================================

class TestFileRef:
    """Test file_ref creation and GC."""

    def test_ensure_extract_dir(self):
        from houdini_extraction.file_ref import ensure_extract_dir
        with patch('houdini_extraction.file_ref.EXTRACT_DIR', tempfile.mkdtemp()):
            path = ensure_extract_dir()
            assert os.path.isdir(path)

    def test_write_file_ref(self):
        from houdini_extraction.file_ref import write_file_ref
        with patch('houdini_extraction.file_ref.EXTRACT_DIR', tempfile.mkdtemp()) as tmp:
            result = write_file_ref(b'test data', '.bin', prefix='test')
            assert result['type'] == 'file_ref'
            assert result['format'] == 'bin'
            assert result['size_bytes'] == 9
            assert result['ttl_seconds'] == 300
            assert os.path.exists(result['path'])

    def test_write_file_ref_pair(self):
        from houdini_extraction.file_ref import write_file_ref_pair
        with patch('houdini_extraction.file_ref.EXTRACT_DIR', tempfile.mkdtemp()):
            result = write_file_ref_pair(
                binary_data=b'\x00\x00\x80?',  # 1.0 as float32
                metadata={'count': 1, 'type': 'float'},
                binary_ext='.bin',
                prefix='test',
            )
            assert result['type'] == 'file_ref'
            assert 'metadata_path' in result
            assert os.path.exists(result['path'])
            assert os.path.exists(result['metadata_path'])

            # Verify metadata sidecar
            with open(result['metadata_path']) as f:
                meta = json.load(f)
            assert meta['count'] == 1

    def test_gc_expired_files(self):
        from houdini_extraction.file_ref import gc_expired_files
        tmp = tempfile.mkdtemp()
        with patch('houdini_extraction.file_ref.EXTRACT_DIR', tmp):
            # Create a file and make it old
            filepath = os.path.join(tmp, 'old_file.bin')
            with open(filepath, 'w') as f:
                f.write('data')
            # Set mtime to 1 hour ago
            old_time = time.time() - 3600
            os.utime(filepath, (old_time, old_time))

            deleted = gc_expired_files(max_age_seconds=300)
            assert deleted == 1
            assert not os.path.exists(filepath)

    def test_gc_keeps_fresh_files(self):
        from houdini_extraction.file_ref import gc_expired_files
        tmp = tempfile.mkdtemp()
        with patch('houdini_extraction.file_ref.EXTRACT_DIR', tmp):
            filepath = os.path.join(tmp, 'fresh_file.bin')
            with open(filepath, 'w') as f:
                f.write('data')

            deleted = gc_expired_files(max_age_seconds=300)
            assert deleted == 0
            assert os.path.exists(filepath)


# =============================================================================
# Invalidation tests
# =============================================================================

class TestInvalidation:
    """Test invalidation event queue."""

    def test_push_and_drain_events(self):
        from houdini_extraction.invalidation import _event_queue, _push_event, drain_events

        # Clear queue first
        _event_queue.clear()

        _push_event('parm_changed', scope='node', path='/obj/geo1/scatter1')
        _push_event('node_created', scope='network', path='/obj/geo2')

        events = drain_events()
        assert len(events) == 2
        assert events[0]['event_type'] == 'parm_changed'
        assert events[0]['path'] == '/obj/geo1/scatter1'
        assert events[1]['event_type'] == 'node_created'

        # Queue should be empty after drain
        assert drain_events() == []

    def test_handle_drain_events(self):
        from houdini_extraction.invalidation import (
            _event_queue,
            _push_event,
            handle_drain_events,
        )

        _event_queue.clear()
        _push_event('hip_saved', scope='scene', path='/tmp/test.hip')

        result = handle_drain_events({})
        assert result['count'] == 1
        assert result['events'][0]['event'] == 'invalidate'

    def test_event_queue_cap(self):
        from houdini_extraction.invalidation import (
            _MAX_QUEUE_SIZE,
            _event_queue,
            _push_event,
            drain_events,
        )

        _event_queue.clear()
        for i in range(_MAX_QUEUE_SIZE + 100):
            _push_event('parm_changed', path=f'/obj/node{i}')

        events = drain_events()
        assert len(events) == _MAX_QUEUE_SIZE


# =============================================================================
# AOV builder test
# =============================================================================

class TestAOVBuilder:
    """Test AOV entry builder."""

    def test_canonical_aov(self):
        from houdini_extraction.aov import _build_aov_entry
        result = _build_aov_entry('depth')
        assert result['aov_name'] == 'depth'
        assert result['aov_type'] == 'DEPTH'
        assert result['data_type'] == 'float'
        assert result['format'] == 'float32'
        assert result['normalize'] is True
        assert result['normalize_range'] == [0.0, 100.0]

    def test_canonical_aov_with_override(self):
        from houdini_extraction.aov import _build_aov_entry
        result = _build_aov_entry('N_world', data_type='vector3f')
        assert result['aov_name'] == 'N_world'
        assert result['data_type'] == 'vector3f'
        assert result['aov_type'] == 'NORMAL_WORLD'

    def test_non_canonical_aov_gets_prefix(self):
        from houdini_extraction.aov import _build_aov_entry
        result = _build_aov_entry('my_custom_pass')
        assert result['aov_name'] == 'custom_my_custom_pass'
        assert result['aov_type'] == 'MY_CUSTOM_PASS'

    def test_already_prefixed_aov(self):
        from houdini_extraction.aov import _build_aov_entry
        result = _build_aov_entry('custom_occlusion')
        assert result['aov_name'] == 'custom_occlusion'
