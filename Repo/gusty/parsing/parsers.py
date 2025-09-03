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

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except (IOError, UnicodeDecodeError):
        return {"metadata": {}, "content": ""}

    # Handle empty files
    if not content:
        return {"metadata": {}, "content": ""}

    # Check if file starts with frontmatter delimiter
    if content.strip().startswith('---'):
        # Split by --- to extract frontmatter
        parts = content.split('---', 2)
        if len(parts) >= 3:
            # parts[0] should be empty or whitespace
            # parts[1] is the YAML metadata
            # parts[2] is the content
            metadata_yaml = parts[1].strip()
            if metadata_yaml:
                try:
                    metadata = yaml.load(metadata_yaml, Loader=loader)
                    if metadata is None:
                        metadata = {}
                except yaml.YAMLError:
                    metadata = {}
            else:
                metadata = {}

            file_content = parts[2].lstrip('\n')  # Remove leading newlines from content
            return {"metadata": metadata, "content": file_content}

    # No frontmatter delimiters found - try to parse the whole file as YAML
    # This is for pure YAML files
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

        # Parse the YAML content
        yaml_spec = yaml.load(yaml_content, Loader=loader)

        # Ensure we return a dictionary
        if isinstance(yaml_spec, dict):
            return yaml_spec
        else:
            return {}
    except (yaml.YAMLError, IOError, UnicodeDecodeError):
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
    try:
        result = frontmatter_load(file_path, loader=loader)
        if result.get("metadata"):
            # Update spec with any metadata found
            spec.update(result["metadata"])
    except Exception:
        pass  # Keep defaults if parsing fails

    return spec


def parse_ipynb(file_path, loader=None):
    """
    Parse Jupyter notebook
    - Extract YAML from first markdown cell that contains "```yaml" or frontmatter
    """
    if loader is None:
        loader = generate_loader()

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)

        # Look for YAML in markdown cells
        for cell in nb.cells:
            if cell.cell_type == 'markdown':
                source = cell.source

                # Check for code block with yaml
                if '```yaml' in source:
                    # Extract YAML from code block
                    yaml_start = source.find('```yaml') + 7
                    yaml_end = source.find('```', yaml_start)
                    if yaml_end > yaml_start:
                        yaml_content = source[yaml_start:yaml_end].strip()
                        if yaml_content:
                            spec = yaml.load(yaml_content, Loader=loader)
                            if isinstance(spec, dict):
                                return spec

                # Check for frontmatter style
                elif source.strip().startswith('---'):
                    parts = source.split('---', 2)
                    if len(parts) >= 3:
                        yaml_content = parts[1].strip()
                        if yaml_content:
                            spec = yaml.load(yaml_content, Loader=loader)
                            if isinstance(spec, dict):
                                return spec
    except (yaml.YAMLError, IOError, nbformat.reader.NotJSONError, UnicodeDecodeError, KeyError):
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

    try:
        result = frontmatter_load(file_path, loader=loader)

        spec = {}
        if result.get("metadata"):
            spec.update(result["metadata"])

        # Add SQL content to spec if present
        if result.get("content"):
            spec["sql"] = result["content"]

        return spec
    except Exception:
        return {}
