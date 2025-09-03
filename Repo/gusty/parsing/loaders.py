import yaml
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


def scalar_to_value(scalar):
    """
    Converts a YAML ScalarNode to its underlying Python value
    """
    if not hasattr(scalar, 'tag'):
        return scalar

    if scalar.tag == 'tag:yaml.org,2002:str':
        return scalar.value
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


def node_converter(node):
    """
    Converts YAML nodes of varying types into Python values,
    lists, and dictionaries
    """
    if isinstance(node, yaml.ScalarNode):
        return scalar_to_value(node)
    elif isinstance(node, yaml.SequenceNode):
        return [node_converter(item) for item in node.value]
    elif isinstance(node, yaml.MappingNode):
        return {node_converter(key): node_converter(value)
                for key, value in node.value}
    else:
        return node


def wrap_yaml(func):
    """Turn a function into one that can be run on a YAML input"""

    def ret(loader, node):
        if isinstance(node, yaml.ScalarNode):
            # If it's a scalar, pass the value directly
            value = scalar_to_value(node)
            if value is not None:
                return func(value)
            else:
                return func()
        elif isinstance(node, yaml.SequenceNode):
            # If it's a sequence, pass all values as arguments
            args = [node_converter(item) for item in node.value]
            return func(*args)
        elif isinstance(node, yaml.MappingNode):
            # If it's a mapping, pass values as keyword arguments
            kwargs = {node_converter(key): node_converter(value)
                     for key, value in node.value}
            return func(**kwargs)
        else:
            # Default case - call without arguments
            return func()

    return ret


def generate_loader(custom_constructors={}):
    """Generates a SafeLoader with both standard Airflow and custom constructors"""
    loader = yaml.SafeLoader

    # Standard Airflow constructors
    dag_yaml_tags = {
        "!days_ago": days_ago,
        "!timedelta": timedelta,
        "!datetime": datetime,
    }

    # Process custom constructors
    if isinstance(custom_constructors, list) and len(custom_constructors) > 0:
        custom_constructors = {
            ("!" + func.__name__): func for func in custom_constructors
        }

    # Update with custom constructors if provided
    if custom_constructors and len(custom_constructors) > 0:
        dag_yaml_tags.update(custom_constructors)

    # Add all constructors to the loader
    for tag, constructor in dag_yaml_tags.items():
        loader.add_constructor(tag, wrap_yaml(constructor))

    return loader
