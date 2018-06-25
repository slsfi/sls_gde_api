from flask import Blueprint, jsonify, request
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import db_engine, metadata, project_permission_required

group_tools = Blueprint("group_tools", __name__)


@group_tools.route("/<project>/publication_groups")
@project_permission_required
def list_publication_groups(project):
    """
    List all available publication groups
    """
    connection = db_engine.connect()
    groups = Table("publicationGroup", metadata, autoload=True, autoload_with=db_engine)
    statement = select([groups.c.id, groups.c.published, groups.c.name])
    rows = connection.execute(statement).fetchall()
    result = dict(rows[0])
    connection.close()
    return jsonify(result)


@group_tools.route("/<project>/publication_group/<group_id>")
@project_permission_required
def get_publication_group(project, group_id):
    """
    Get all data for a single publication group
    """
    connection = db_engine.connect()
    groups = Table("publicationGroup", metadata, autoload=True, autoload_with=db_engine)
    statement = select([groups]).where(groups.c.id == int(group_id))
    rows = connection.execute(statement).fetchall()
    result = dict(rows[0])
    connection.close()
    return jsonify(result)


@group_tools.route("/<project>/publication_group/<group_id>/publications")
@project_permission_required
def get_publications_in_group(project, group_id):
    """
    List all publications in a given publicationGroup
    """
    connection = db_engine.connect()
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publications.c.id, publications.c.name]).where(publications.c.publicationGroup_id == int(group_id))
    result = []
    for row in connection.execute(statement).fetchall():
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@group_tools.route("/<project>/publication/<publication_id>/add_group", methods=["POST"])
@project_permission_required
def add_publication_to_group(project, publication_id):
    """
    Add a publication to a publicationGroup

    POST data MUST be in JSON format

    POST data MUST contain the following:
    group_id: numerical ID for the publicationGroup
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "group_id" not in request_data:
        return jsonify({"msg": "group_id not in POST data."}), 400

    group_id = int(request_data["group_id"])

    connection = db_engine.connect()
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    statement = publications.update().where(publications.c.id == int(publication_id)).values(publicationGroup_id=group_id)
    transaction = connection.begin()
    try:
        connection.execute(statement)
        statement = select([publications]).where(publications.c.id == int(publication_id))
        updated = dict(connection.execute(statement).fetchone())
        transaction.commit()
        result = {
            "msg": "Updated publication object",
            "row": updated
        }
        return jsonify(result)
    except Exception as e:
        transaction.rollback()
        result = {
            "msg": "Failed to create new object",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@group_tools.route("/<project>/publication_group/new", methods=["POST"])
@project_permission_required
def add_new_publication_group(project):
    """
    Create a new publicationGroup

    POST data MUST be in JSON format

    POST data SHOULD contain the following:
    name: name for the group
    published: publication status for the group, 0 meaning unpublished
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    groups = Table("publicationGroup", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    insert = groups.insert()
    new_group = {
        "name": request_data.get("name", None),
        "published": request_data.get("published", 0)
    }
    try:
        result = connection.execute(insert, **new_group)
        new_row = select([groups]).where(groups.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new group with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new group",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()
