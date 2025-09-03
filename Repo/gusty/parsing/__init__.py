import os
import inspect
from gusty.parsing.loaders import generate_loader
from gusty.parsing.parsers import parse_generic, parse_py, parse_ipynb, parse_sql

default_parsers = {
    ".yml": parse_generic,
    ".yaml": parse_generic,
    ".py": parse_py,
    ".ipynb": parse_ipynb,
    ".sql": parse_sql,
    ".Rmd": parse_generic,
    ".txt": parse_generic,
}


def parse(file_path, parse_dict=default_parsers, loader=None):
    """
    Reading in yaml specs / frontmatter.
    """
    if loader is None:
        loader = generate_loader()

    path, extension = os.path.splitext(file_path)

    # Get the task_id from the filename (without extension)
    task_id = os.path.basename(path)

    # Select the appropriate parser based on file extension
    parser = parse_dict.get(extension, parse_generic)

    # Call the parser with loader if it accepts it
    if "loader" in inspect.signature(parser).parameters.keys():
        yaml_file = parser(file_path, loader=loader)
    else:
        yaml_file = parser(file_path)

    # Ensure yaml_file is a dictionary
    if not isinstance(yaml_file, dict):
        yaml_file = {}

    # gusty always supplies a task_id and a file_path in a spec
    yaml_file["task_id"] = task_id
    yaml_file["file_path"] = file_path

    return yaml_file
