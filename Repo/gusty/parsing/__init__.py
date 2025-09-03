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
}


def parse(file_path, parse_dict=default_parsers, loader=None):
    """
    Reading in yaml specs / frontmatter.
    """
    try:
        if not os.path.exists(file_path):
            return {"task_id": os.path.basename(os.path.splitext(file_path)[0]),
                    "file_path": file_path}

        if loader is None:
            loader = generate_loader()

        path, extension = os.path.splitext(file_path)

        # Get base filename without path or extension for task_id
        task_id = os.path.basename(path)

        # Select parser based on extension (case-insensitive)
        parser = parse_dict.get(extension.lower(), parse_generic)

        # Call appropriate parser
        if "loader" in inspect.signature(parser).parameters.keys():
            yaml_file = parser(file_path, loader=loader)
        else:
            yaml_file = parser(file_path)

        # Ensure yaml_file is a dictionary
        if yaml_file is None or not isinstance(yaml_file, dict):
            yaml_file = {}

        # gusty always supplies a task_id and a file_path in a spec
        yaml_file["task_id"] = task_id
        yaml_file["file_path"] = file_path

        return yaml_file
    except Exception as e:
        # In case of any error, return a minimal valid spec
        return {
            "task_id": os.path.basename(os.path.splitext(file_path)[0]),
            "file_path": file_path
        }
