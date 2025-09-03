import yaml
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


def scalar_to_value(scalar):
    """
    Converts a YAML ScalarNode to its underlying Python value
    """
    if isinstance(scalar, yaml.ScalarNode):
        if scalar.tag == 'tag:yaml.org,2002:str':
            return scalar.value
        elif scalar.tag == 'tag:yaml.org,2002:int':
            return int(scalar.value)
        elif scalar.tag == 'tag:yaml.org,2002:float':
            return float(scalar.value)
        elif scalar.tag == 'tag:yaml.org,2002:bool':
            return scalar.value.lower() in ('true', 'yes', '1')
        elif scalar.tag == 'tag:yaml.org,2002:null':
            return None
        else:
            # Try to parse as int or float, otherwise return as string
            try:
                return int(scalar.value)
            except ValueError:
                try:
                    return float(scalar.value)
                except ValueError:
                    return scalar.value
    return scalar


def node_converter(x):
    """
    Converts YAML nodes of varying types into Python values,
    lists, and dictionaries
    """
    if isinstance(x, yaml.ScalarNode):
        return scalar_to_value(x)
    elif isinstance(x, yaml.SequenceNode):
        return [node_converter(item) for item in x.value]
    elif isinstance(x, yaml.MappingNode):
        result = {}
        for key_node, value_node in x.value:
            key = node_converter(key_node)
            value = node_converter(value_node)
            result[key] = value
        return result
    else:
        return x


def wrap_yaml(func):
    """Turn a function into one that can be run on a YAML input"""

    def ret(loader, x):
        if isinstance(x, yaml.ScalarNode):
            val = loader.construct_scalar(x)

            # Special handling for timedelta strings like 'minutes: 5'
            if func.__name__ == 'timedelta' and isinstance(val, str):
                # Parse string like 'minutes: 5' or 'hours: 2'
                if ':' in val:
                    parts = val.split(':', 1)
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        try:
                            value = int(value)
                            return func(**{key: value})
                        except ValueError:
                            try:
                                value = float(value)
                                return func(**{key: value})
                            except ValueError:
                                pass

            # Try to convert to appropriate type
            try:
                val = int(val)
            except (ValueError, TypeError):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    pass

            # For parameterless functions
            if val is None or val == '':
                return func()
            else:
                return func(val)
        elif isinstance(x, yaml.SequenceNode):
            args = loader.construct_sequence(x)
            return func(*args)
        elif isinstance(x, yaml.MappingNode):
            kwargs = loader.construct_mapping(x)
            return func(**kwargs)
        else:
            return func()

    return ret


def generate_loader(custom_constructors={}):
    """Generates a SafeLoader with both standard Airflow and custom constructors"""
    # Create a new loader class inheriting from SafeLoader
    class CustomLoader(yaml.SafeLoader):
        pass

    loader = CustomLoader

    # Standard Airflow constructors with correct tag names
    dag_yaml_tags = {
        "!days_ago": days_ago,
        "!timedelta": timedelta,
        "!datetime": datetime,
    }

    # Handle custom constructors
    if isinstance(custom_constructors, list) and len(custom_constructors) > 0:
        custom_constructors = {
            ("!" + func.__name__): func for func in custom_constructors
        }

    # Merge custom constructors
    if custom_constructors and len(custom_constructors) > 0:
        dag_yaml_tags.update(custom_constructors)

    # Register all constructors with the loader
    for tag, constructor in dag_yaml_tags.items():
        loader.add_constructor(tag, wrap_yaml(constructor))

    return loader
