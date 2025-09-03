import yaml, ast, importlib.util, nbformat, jupytext, re
from gusty.parsing.loaders import generate_loader
from gusty.importing import airflow_version

if airflow_version > 1:
    from airflow.operators.python import PythonOperator
else:
    from airflow.operators.python_operator import PythonOperator


def frontmatter_load(file_path, loader=None):
    """
    Loads YAML frontmatter. Expects a YAML block at the top of the file
    that starts and ends with "---". In use in favor of frontmatter.load
    so that custom dag_constructors (via PyYaml) can be used uniformly across
    all file types.
    """
    return {"metadata": None, "content": None}


def parse_generic(file_path, loader=None):
    return {}


def parse_py(file_path, loader=None):
    return {"operator": "nonexistent.operator"}


def parse_ipynb(file_path, loader=None):
    return {}


def parse_sql(file_path, loader=None):
    return {}
