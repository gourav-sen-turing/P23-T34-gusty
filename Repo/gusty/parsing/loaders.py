import yaml
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


def scalar_to_value(scalar):
    """
    Converts a YAML ScalarNode to its underlying Python value
    """
    if not hasattr(scalar, 'tag') or not hasattr(scalar, 'value'):
        return scalar

    try:
        if scalar.tag == 'tag:yaml.org,2002:str':
            return str(scalar.value)
        elif scalar.tag == 'tag:yaml.org,2002:int':
            return int(scalar.value)
        elif scalar.tag == 'tag:yaml.org,2002:float':
            return float(scalar.value)
        elif scalar.tag == 'tag:yaml.org,2002:bool':
            return scalar.value.lower() == 'true'
        elif scalar.tag == 'tag:yaml.org,2002:null':
            return None
        else:
            return scalar.value
    except (ValueError, TypeError, AttributeError):
        # Fallback to returning the raw value if conversion fails
        return scalar.value if hasattr(scalar, 'value') else scalar


def node_converter(node):
    """
    Converts YAML nodes of varying types into Python values,
    lists, and dictionaries
    """
    try:
        if isinstance(node, yaml.ScalarNode):
            return scalar_to_value(node)
        elif isinstance(node, yaml.SequenceNode):
            return [node_converter(item) for item in node.value]
        elif isinstance(node, yaml.MappingNode):
            return {node_converter(key): node_converter(value)
                    for key, value in node.value}
        else:
            return node
    except Exception:
        # Fallback for any unexpected errors
        return node


def wrap_yaml(func):
    """Turn a function into one that can be run on a YAML input"""
    def ret(loader, node):
        try:
            if isinstance(node, yaml.ScalarNode):
                # If it's a scalar, pass the value directly
                value = loader.construct_scalar(node)
                if value is not None and value != '':
                    return func(value)
                else:
                    return func()
            elif isinstance(node, yaml.SequenceNode):
                # If it's a sequence, pass all values as arguments
                args = loader.construct_sequence(node)
                return func(*args)
            elif isinstance(node, yaml.MappingNode):
                # If it's a mapping, pass values as keyword arguments
                kwargs = loader.construct_mapping(node)
                return func(**kwargs)
            else:
                # Default case - call without arguments
                return func()
        except Exception:
            # Return the function with no args as fallback
            try:
                return func()
            except Exception:
                # Last resort - return None to avoid complete failure
                return None

    return ret


def generate_loader(custom_constructors={}):
    """Generates a SafeLoader with both standard Airflow and custom constructors"""
    # Create a new class inheriting from SafeLoader to avoid modifying the global class
    class CustomLoader(yaml.SafeLoader):
        pass

    # Standard Airflow constructors
    dag_yaml_tags = {
        "!days_ago": days_ago,
        "!timedelta": timedelta,
        "!datetime": datetime,
    }

    # Process custom constructors if it's a list
    if isinstance(custom_constructors, list) and len(custom_constructors) > 0:
        custom_constructors = {
            ("!" + func.__name__): func for func in custom_constructors
        }

    # Update with custom constructors if provided
    if custom_constructors and len(custom_constructors) > 0:
        dag_yaml_tags.update(custom_constructors)

    # Add all constructors to the loader
    for tag, constructor in dag_yaml_tags.items():
        CustomLoader.add_constructor(tag, wrap_yaml(constructor))

    return CustomLoader
