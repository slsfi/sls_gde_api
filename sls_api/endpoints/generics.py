from flask import jsonify
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request
from functools import wraps
import io
import logging
import os
from ruamel.yaml import YAML
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select

metadata = MetaData()

logger = logging.getLogger("sls_api.de_tools")

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as config:
    yaml = YAML(typ="safe")
    config = yaml.load(config)
    db_engine = create_engine(config["engine"], pool_pre_ping=True)
    elastic_config = config["elasticsearch_connection"]


def get_project_config(project_name):
    if project_name in config:
        return config[project_name]
    return None


def project_permission_required(fn):
    """
    Function decorator that checks for JWT authorization and that the user has edit rights for the project.
    The project the method concerns should be the first positional argument or a keyword argument.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_in_request()
        identity = get_jwt_identity()
        if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and identity["sub"] == "test@test.com":
            # If in FLASK_DEBUG mode, test@test.com user has access to all projects
            return fn(*args, **kwargs)
        else:
            if len(args) > 0:
                if args[0] in identity["projects"]:
                    return fn(*args, **kwargs)
            elif "projects" in kwargs:
                if kwargs["projects"] in identity["projects"]:
                    return fn(*args, **kwargs)
            else:
                return jsonify({"msg": "No access to this project."}), 403
    return wrapper


def get_project_id_from_name(project):
    projects = Table('project', metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([projects.c.id]).where(projects.c.name == project)
    project_id = connection.execute(statement).fetchone()
    connection.close()
    try:
        return int(project_id["id"])
    except Exception:
        return None


def select_all_from_table(table_name):
    table = Table(table_name, metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    rows = connection.execute(select([table])).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)
