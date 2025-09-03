import os, sys, pkgutil, itertools, airflow, importlib
from inflection import underscore

############
## Params ##
############

airflow_version = int(str(airflow.__version__)[0])

###########################
## Operator Import Logic ##
###########################


def get_operator_location(operator_string):
    """
    Get package name / determine if "local" keyword is used
    """
    if not operator_string or not isinstance(operator_string, str):
        return None

    parts = operator_string.split(".")
    if len(parts) > 0:
        return parts[0]
    return None


def get_operator_name(operator_string):
    """
    Get operator class
    """
    if not operator_string or not isinstance(operator_string, str):
        return None

    parts = operator_string.split(".")
    if len(parts) > 0:
        return parts[-1]
    return None


def get_operator_module(operator_string):
    """
    Get module name
    """
    if not operator_string or not isinstance(operator_string, str):
        return None

    parts = operator_string.split(".")
    if len(parts) > 1:
        # Everything except the last part (class name)
        return ".".join(parts[:-1])
    return None


# Add $AIRFLOW_HOME/operators directory to path for local.operator syntax to work

gusty_home = os.environ.get("GUSTY_HOME", "")
if gusty_home == "":
    CUSTOM_OPERATORS_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", ""), "operators")
else:
    CUSTOM_OPERATORS_DIR = os.path.join(gusty_home, "operators")

sys.path.append(CUSTOM_OPERATORS_DIR)
module_paths = [("", [CUSTOM_OPERATORS_DIR])]
pairs = [
    [(_.name, m + _.name) for _ in pkgutil.iter_modules(path)]
    for m, path in module_paths
]
module_dict = dict(itertools.chain(*pairs))


def get_operator(operator_string):
    """
    Given an operator string, determine the location of that operator and return the operator object
    """
    if not operator_string:
        # Default to DummyOperator
        operator_string = "airflow.operators.dummy.DummyOperator"

    # Get module and class names
    module_name = get_operator_module(operator_string)
    class_name = get_operator_name(operator_string)
    location = get_operator_location(operator_string)

    if not module_name or not class_name:
        raise ImportError(f"Invalid operator string: {operator_string}")

    # Handle local operators
    if location == "local":
        # Try to import from local operators directory
        if module_name in module_dict:
            try:
                module = importlib.import_module(module_dict[module_name])
                operator = getattr(module, class_name)
                return operator
            except (ImportError, AttributeError) as e:
                raise ImportError(f"Cannot import local operator {operator_string}: {e}")

    # Try to import normally
    try:
        module = importlib.import_module(module_name)
        operator = getattr(module, class_name)
        return operator
    except (ImportError, AttributeError) as e:
        # Try with underscored module name (for backwards compatibility)
        try:
            underscored_module = underscore(module_name)
            module = importlib.import_module(underscored_module)
            operator = getattr(module, class_name)
            return operator
        except (ImportError, AttributeError):
            raise ImportError(f"Cannot import operator {operator_string}: {e}")
