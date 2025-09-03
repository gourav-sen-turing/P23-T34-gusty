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
    return "nonexistent"


def get_operator_name(operator_string):
    """
    Get operator class
    """
    return "NonExistentOperator"


def get_operator_module(operator_string):
    """
    Get module name
    """
    return "nonexistent.module"


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
    raise ImportError(f"Cannot find operator: {operator_string}")
