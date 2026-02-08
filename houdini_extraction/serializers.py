"""
Contract-compliant serializers for Houdini data.

Maps Houdini parameter template types to the pixel_vision interface contract
vocabulary (§2.1, §2.2). These serializers coexist with the existing bridge
serializers — no modifications to _serialize_node/_serialize_parm.
"""

from __future__ import annotations

from typing import Any

# Houdini module — only available inside Houdini
try:
    import hou
    IN_HOUDINI = True
except ImportError:
    IN_HOUDINI = False
    hou = None


# ============================================================================
# Houdini parmTemplateType → contract type mapping
# ============================================================================

# Map (hou.parmTemplateType name, num_components, naming_scheme) → contract type
_PARM_TYPE_MAP: dict[str, str] = {
    'Int': 'int',
    'Float': 'float',
    'String': 'string',
    'Toggle': 'toggle',
    'Menu': 'menu',
    'Button': 'button',
    'FolderSet': 'data',
    'Folder': 'data',
    'Separator': 'data',
    'Label': 'data',
    'Data': 'data',
    'Ramp': 'ramp',
}

# Interp enum name → contract interp string
_RAMP_INTERP_MAP: dict[str, str] = {
    'Constant': 'constant',
    'Linear': 'linear',
    'CatmullRom': 'catmull-rom',
    'MonotoneCubic': 'monotone-cubic',
    'Bezier': 'bezier',
}


def _classify_parm_type(template: Any) -> str:
    """Map a Houdini parm template to a contract type string.

    Uses template type, component count, and naming scheme to determine
    the correct contract vocabulary type (§2.2).
    """
    type_name = template.type().name()

    # Handle ramp specially
    if type_name == 'Ramp':
        return 'ramp'

    # Handle multiparm folders
    if type_name == 'Folder':
        try:
            folder_type = template.folderType().name()
            if 'Multiparm' in folder_type or 'MultiparmBlock' in folder_type:
                return 'multiparm'
        except Exception:
            pass
        return 'data'

    # Handle Float with multiple components
    if type_name == 'Float':
        num = template.numComponents()
        scheme = template.namingScheme().name() if hasattr(template, 'namingScheme') else ''

        if num == 2:
            return 'vector2'
        if num == 3:
            # Check naming scheme for color vs vector
            if 'RGBA' in scheme or 'Color' in scheme:
                return 'color'
            return 'vector3'
        if num == 4:
            if 'RGBA' in scheme or 'Color' in scheme:
                return 'color_alpha'
            return 'vector4'
        return 'float'

    # Handle Int with multiple components (rare but possible)
    if type_name == 'Int':
        num = template.numComponents()
        if num == 2:
            return 'vector2'
        if num == 3:
            return 'vector3'
        if num == 4:
            return 'vector4'
        return 'int'

    # Handle String with file chooser
    if type_name == 'String':
        try:
            if template.stringType().name() == 'FileReference':
                return 'file'
        except Exception:
            pass
        return 'string'

    return _PARM_TYPE_MAP.get(type_name, 'data')


def serialize_parm_contract(parm: Any) -> dict[str, Any]:
    """Serialize a Houdini parameter to the contract §2.2 schema.

    Args:
        parm: A hou.Parm or hou.ParmTuple instance.

    Returns:
        Dictionary matching the Parameter Value schema (§2.2).
    """
    # Determine if we have a ParmTuple or single Parm
    is_tuple = hasattr(parm, '__len__') and hasattr(parm, 'parmTemplate')

    if is_tuple:
        template = parm.parmTemplate()
        name = parm.name()
    else:
        template = parm.parmTemplate()
        name = parm.name()
        # For component parms (tx, ty, tz), get the tuple
        parm_tuple = parm.tuple()
        if parm_tuple and len(parm_tuple) > 1:
            return serialize_parm_contract(parm_tuple)

    contract_type = _classify_parm_type(template)
    label = template.label()

    # Get value
    value: Any
    if contract_type == 'ramp':
        value = _serialize_ramp(parm)
    elif contract_type == 'multiparm':
        value = _serialize_multiparm(parm, template)
    elif contract_type == 'button':
        value = None
    elif is_tuple:
        value = list(parm.eval())
    else:
        value = parm.eval()

    # Get default
    default: Any = None
    try:
        if is_tuple:
            default = list(template.defaultValue())
        else:
            dv = template.defaultValue()
            default = dv[0] if isinstance(dv, tuple) and len(dv) == 1 else dv
    except Exception:
        pass

    # Build result
    result: dict[str, Any] = {
        'name': name,
        'label': label,
        'type': contract_type,
        'value': value,
        'default': default,
        'is_default': parm.isAtDefault() if hasattr(parm, 'isAtDefault') else None,
        'has_expression': False,
        'expression': None,
        'expression_language': None,
        'min': None,
        'max': None,
        'menu_items': None,
        'tuple_size': template.numComponents() if hasattr(template, 'numComponents') else 1,
    }

    # Expression info (single parm only)
    if not is_tuple and hasattr(parm, 'expression'):
        try:
            result['expression'] = parm.expression()
            result['expression_language'] = parm.expressionLanguage().name().lower()
            result['has_expression'] = True
        except Exception:
            pass

    # Min/max
    try:
        if hasattr(template, 'minValue'):
            result['min'] = template.minValue()
        if hasattr(template, 'maxValue'):
            result['max'] = template.maxValue()
    except Exception:
        pass

    # Menu items
    try:
        if hasattr(template, 'menuItems') and template.menuItems():
            result['menu_items'] = list(template.menuItems())
    except Exception:
        pass

    return result


def _serialize_ramp(parm: Any) -> dict[str, Any]:
    """Serialize a ramp parameter to contract §2.2.1 schema.

    Args:
        parm: A hou.Parm for a ramp parameter.

    Returns:
        Ramp value dict with ramp_type and keys array.
    """
    try:
        ramp = parm.eval() if not hasattr(parm, 'evalAsRamp') else parm.evalAsRamp()
        if hasattr(parm, 'evalAsRamp'):
            ramp = parm.evalAsRamp()

        is_color = ramp.isColor() if hasattr(ramp, 'isColor') else False
        ramp_type = 'color' if is_color else 'float'

        keys = []
        for i in range(len(ramp.keys())):
            basis_name = str(ramp.basis()[i])
            # Strip enum prefix
            basis_short = basis_name.rsplit('.', 1)[-1] if '.' in basis_name else basis_name
            interp = _RAMP_INTERP_MAP.get(basis_short, 'linear')

            key_data: dict[str, Any] = {
                'pos': ramp.keys()[i],
                'interp': interp,
            }

            if is_color:
                key_data['value'] = list(ramp.values()[i])
            else:
                key_data['value'] = ramp.values()[i]

            keys.append(key_data)

        return {'ramp_type': ramp_type, 'keys': keys}
    except Exception:
        return {'ramp_type': 'float', 'keys': []}


def _serialize_multiparm(parm: Any, template: Any) -> dict[str, Any]:
    """Serialize a multiparm parameter to contract §2.2.2 schema.

    Args:
        parm: A hou.Parm for the multiparm count parameter.
        template: The parm template (Folder with MultiparmBlock type).

    Returns:
        Multiparm value dict with count and instances array.
    """
    try:
        # The parm for a multiparm folder is the instance count
        count = int(parm.eval()) if hasattr(parm, 'eval') else 0

        # Get the child parm templates that define each instance
        child_templates = template.parmTemplates() if hasattr(template, 'parmTemplates') else []
        base_names = [t.name() for t in child_templates if hasattr(t, 'name')]

        # Read each instance's parameters
        node = parm.node() if hasattr(parm, 'node') else None
        instances: list[dict[str, Any]] = []

        if node and base_names:
            for i in range(1, count + 1):
                instance: dict[str, Any] = {}
                for base in base_names:
                    # Multiparm instance parms are named base + index (e.g., layer_name_1)
                    inst_name = f'{base}{i}'
                    inst_parm = node.parm(inst_name)
                    if inst_parm is not None:
                        instance[inst_name] = inst_parm.eval()
                    else:
                        # Try without separator (some use base1, base2 pattern)
                        inst_name_alt = f'{base}_{i}'
                        inst_parm = node.parm(inst_name_alt)
                        if inst_parm is not None:
                            instance[inst_name_alt] = inst_parm.eval()
                instances.append(instance)

        return {'count': count, 'instances': instances}
    except Exception:
        return {'count': 0, 'instances': []}


def serialize_node_contract(node: Any) -> dict[str, Any]:
    """Serialize a Houdini node to the contract §2.1 Scene Graph Node schema.

    Args:
        node: A hou.Node instance.

    Returns:
        Dictionary matching the Scene Graph Node schema (§2.1).
    """
    # Inputs as absolute paths (never null — empty list for unconnected)
    inputs: list[str] = []
    for inp in node.inputs():
        if inp is not None:
            inputs.append(inp.path())

    # Outputs as absolute paths
    outputs: list[str] = []
    for out in node.outputs():
        outputs.append(out.path())

    # Flags
    flags: dict[str, bool] = {}
    if hasattr(node, 'isDisplayFlagSet'):
        flags['display'] = node.isDisplayFlagSet()
    if hasattr(node, 'isRenderFlagSet'):
        flags['render'] = node.isRenderFlagSet()
    if hasattr(node, 'isBypassed'):
        flags['bypass'] = node.isBypassed()
    if hasattr(node, 'isTemplateFlagSet'):
        flags['template'] = node.isTemplateFlagSet()

    # Errors and warnings
    errors: list[str] = []
    if hasattr(node, 'errors'):
        for e in node.errors():
            errors.append(e if isinstance(e, str) else e.text())

    warnings: list[str] = []
    if hasattr(node, 'warnings'):
        for w in node.warnings():
            warnings.append(w if isinstance(w, str) else w.text())

    # Time dependence
    is_time_dependent = False
    try:
        is_time_dependent = node.isTimeDependent()
    except Exception:
        pass

    return {
        'path': node.path(),
        'type': node.type().name(),
        'category': node.type().category().name(),
        'name': node.name(),
        'parent': node.parent().path() if node.parent() else '/',
        'children': [c.path() for c in node.children()] if hasattr(node, 'children') else [],
        'inputs': inputs,
        'outputs': outputs,
        'flags': flags,
        'errors': errors,
        'warnings': warnings,
        'is_time_dependent': is_time_dependent,
    }


# ============================================================================
# Attribute type mapping (Houdini attrib type → contract type)
# ============================================================================

def classify_attrib_type(attrib: Any) -> tuple[str, int]:
    """Map a Houdini geometry attribute to contract type and tuple size.

    Args:
        attrib: A hou.Attrib instance.

    Returns:
        Tuple of (contract_type_string, tuple_size).
    """
    data_type = attrib.dataType().name()  # 'Float', 'Int', 'String'
    size = attrib.size()

    if data_type == 'Float':
        if size == 1:
            return ('float', 1)
        if size == 2:
            return ('vector2', 2)
        if size == 3:
            # Check if it's a color attribute by name convention
            if attrib.name() in ('Cd', 'basecolor', 'diffuse_color'):
                return ('color', 3)
            return ('vector3', 3)
        if size == 4:
            if attrib.name() in ('Cd',):
                return ('color_alpha', 4)
            return ('vector4', 4)
        return ('float', size)

    if data_type == 'Int':
        if size == 1:
            return ('int', 1)
        return ('int', size)

    if data_type == 'String':
        return ('string', 1)

    return ('data', size)
