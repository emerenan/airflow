# 
# created by @erenansouza
# Last update was in 2023-02-08
# 
from typing import Dict, Optional

def render_jinja(path_base:str,sql_path: str, sql_params: Optional[Dict] = None) -> str:
    """ Function created to render jinja templates (sql).

    Args:
        path_base: Path of the DAG
        sql_path: Path to the flader that contains teh sql queries.
        sql_params: A dictionary which contains the parameter to replace when the query will be rendered.

    Returns:
        Query rendered in string format.
    """
    from jinja2 import FileSystemLoader, Environment
    
    loader_template = FileSystemLoader(searchpath=path_base)
    templateEnv = Environment(loader=loader_template)
    TEMPLATE_FILE = sql_path
    template = templateEnv.get_template(TEMPLATE_FILE)
    
    query = template.render(sql_params) if sql_params else template.render()
    
    return query