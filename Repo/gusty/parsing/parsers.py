import yaml, ast, importlib.util, nbformat, jupytext, re, os
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
    if loader is None:
        loader = yaml.SafeLoader

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Check if file starts with frontmatter delimiter
    if content.strip().startswith('---'):
        parts = content.split('---', 2)
        if len(parts) >= 3:
            # Extract metadata and content
            metadata_yaml = parts[1].strip()
            metadata = yaml.load(metadata_yaml, Loader=loader)
            file_content = parts[2].strip()
            return {"metadata": metadata, "content": file_content}

    # If no frontmatter, try to load the whole file as YAML
    try:
        metadata = yaml.load(content, Loader=loader)
        if isinstance(metadata, dict):
            return {"metadata": metadata, "content": None}
    except yaml.YAMLError:
        pass

    # Default return if no valid YAML found
    return {"metadata": {}, "content": content}


def parse_generic(file_path, loader=None):
    """
    Parse a generic file (assumes the entire file is YAML)
    """
    if loader is None:
        loader = generate_loader()

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml_content = f.read()
        yaml_spec = yaml.load(yaml_content, Loader=loader)
        if isinstance(yaml_spec, dict):
            return yaml_spec
    except (yaml.YAMLError, IOError):
        pass

    # If loading failed or result is not a dict, return empty dict
    return {}


def parse_py(file_path, loader=None):
    """
    Parse Python file
    - For regular Python files, extract any yaml block at the beginning
    - Extract and use any operator and other parameters defined
    """
    if loader is None:
        loader = generate_loader()

    # Default operator based on Airflow version
    default_operator = "airflow.operators.python.PythonOperator" if airflow_version > 1 else "airflow.operators.python_operator.PythonOperator"

    # Default spec with python_callable_path
    spec = {
        "operator": default_operator,
        "python_callable_path": file_path
    }

    # Try to extract frontmatter if available
    result = frontmatter_load(file_path, loader=loader)
    if result["metadata"]:
        # Update spec with any metadata found
        spec.update(result["metadata"])

    return spec


def parse_ipynb(file_path, loader=None):
    """
    Parse Jupyter notebook
    - Extract YAML from first markdown cell that contains "```yaml" or frontmatter
    """
    if loader is None:
        loader = generate_loader()

    try:
        nb = nbformat.read(file_path, as_version=4)

        # Look for YAML in markdown cells
        yaml_content = None
        for cell in nb.cells:
            if cell.cell_type == 'markdown':
                source = cell.source

                # Check for code block with yaml
                if '```yaml' in source:
                    yaml_blocks = source.split('```yaml')
                    if len(yaml_blocks) > 1:
                        yaml_content = yaml_blocks[1].split('```')[0].strip()
                        break

                # Check for frontmatter style
                elif source.strip().startswith('---'):
                    parts = source.split('---', 2)
                    if len(parts) >= 3:
                        yaml_content = parts[1].strip()
                        break

        if yaml_content:
            spec = yaml.load(yaml_content, Loader=loader)
            if isinstance(spec, dict):
                return spec
    except (yaml.YAMLError, IOError, nbformat.reader.NotJSONError):
        pass

    # Default return if no valid YAML found
    return {}


def parse_sql(file_path, loader=None):
    """
    Parse SQL file with frontmatter
    - Extract YAML frontmatter
    - Store SQL content as 'sql' attribute
    """
    if loader is None:
        loader = generate_loader()

    result = frontmatter_load(file_path, loader=loader)

    spec = {}
    if result["metadata"]:
        spec.update(result["metadata"])

    # Add SQL content to spec
    if result["content"]:
        spec["sql"] = result["content"]

    return spec
