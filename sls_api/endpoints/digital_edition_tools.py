from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required, verify_jwt_in_request
from functools import wraps
import io
import logging
import os
from ruamel.yaml import YAML
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
import subprocess

de_tools = Blueprint("digital_edition_tools", __name__)

metadata = MetaData()

logger = logging.getLogger("sls_api.de_tools")

# TODO new config for GDE_tools, since they're working with new database structures
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as config:
    yaml = YAML()
    config = yaml.load(config)
    db_engine = create_engine(config["engine"])


def project_permission_required(fn):
    """
    Function decorator that checks for JWT authorization and that the user has edit rights for the project.
    The project the method concerns should be the first positional argument or a keyword argument.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_in_request()
        identity = get_jwt_identity()
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
    return int(project_id["id"])


@de_tools.route("/<project>/locations/new", methods=["POST"])
@project_permission_required
def add_new_location(project):
    """
    Add a new location object to the database

    POST data MUST be in JSON format.

    POST data MUST contain:
    name: location name

    POST data SHOULD also contain:
    description: location description

    POST data CAN also contain:
    legacyXMLId: legacy XML id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    locations = Table('location', metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_location = {
        "name": request_data["name"],
        "description": request_data.get("desription", None),
        "project_id": get_project_id_from_name(project),
        "legacyXMLId": request_data.get("legacyXMLId", None),
        "latitude": request_data.get("latitude", None),
        "longitude": request_data.get("longitude", None)
    }
    try:
        insert = locations.insert()
        result = connection.execute(insert, **new_location)
        new_row = select([locations]).where(locations.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new location with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new location",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/subjects/new", methods=["POST"])
@project_permission_required
def add_new_subject(project):
    """
    Add a new subject object to the database

    POST data MUST be in JSON format

    POST data SHOULD contain:
    type: subject type
    description: subject descrtiption

    POST data CAN also contain:
    firstName: Subject first or given name
    lastName Subject surname
    preposition: preposition for subject
    fullName: Subject full name
    legacyXMLId: Legacy XML id for subject
    dateBorn: Subject date of birth
    dateDeceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    subjects = Table('subject', metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_subject = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "firstName": request_data.get("firstName", None),
        "lastName": request_data.get("lastName", None),
        "preposition": request_data.get("preposition", None),
        "fullName": request_data.get("fullName", None),
        "legacyXMLId": request_data.get("legacyXMLId", None),
        "dateBorn": request_data.get("dateBorn", None),
        "dateDeceased": request_data.get("dateDeceased", None)
    }
    try:
        insert = subjects.insert()
        result = connection.execute(insert, **new_subject)
        new_row = select([subjects]).where(subjects.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new subject with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new subject.",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/tags/new", methods=["POST"])
@project_permission_required
def add_new_tag(project):
    """
    Add a new tag object to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: tag type
    name: tag name

    POST data CAN also contain:
    description: tag description
    legacyXMLId: Legacy XML id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    tags = Table("tag", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_tag = {
        "type": request_data.get("type", None),
        "name": request_data.get("name", None),
        "project_id": get_project_id_from_name(project),
        "description": request_data.get("description", None),
        "legacyXMLId": request_data.get("legacyXMLId", None)
    }
    try:
        insert = tags.insert()
        result = connection.execute(insert, **new_tag)
        new_row = select([tags]).where(tags.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new subject with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new tag",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/events/")
@jwt_required
def get_events():
    """
    Get a list of all available events in the database
    """
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([events])
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result), 200


@de_tools.route("/events/new", methods=["POST"])
@jwt_required
def add_new_event():
    """
    Add a new event to the database, optionally connecting it to a location, subject, or tag using eventConnection
    """
    pass


@de_tools.route("/event_occurances/")
@de_tools.route("/event_occurances/<event_id>")
@jwt_required
def get_event_occurances(event_id=None):
    """
    Get a list of all eventOccurances in the database, optionally limiting to a given event
    """
    event_occurances = Table("eventOccurance", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    if event_id is None:
        statement = select([event_occurances])
    else:
        statement = select([event_occurances]).where(event_occurances.c.event_id == event_id)
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result), 200


@de_tools.route("/event_occurances/new", methods=["POST"])
@jwt_required
def new_event_occurance():
    """
    Add a new eventOccurance to the database
    """
    pass


@de_tools.route("/<project>/update_xml/by_path/<file_path>", methods=["POST", "UPDATE"])
@project_permission_required
def update_file_in_remote(project, file_path):
    """
    Add new XML or update existing XML in git remote
    """
    pass


@de_tools.route("/<project>/get_latest_file/by_path/<file_path>")
@project_permission_required
def get_file_from_remote(project, file_path):
    """
    Get latest XML file from git remote
    """
    pass


@de_tools.route("/<project>/get_tree/")
@de_tools.route("/<project>/get_tree/<file_path>")
@project_permission_required
def get_file_tree_from_remote(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    pass


@de_tools.route("/<project>/fascimile_collection/new", methods=["POST"])
@project_permission_required
def create_fascimile_collection(project):
    """
    Create a new publicationFascimileCollection
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list")
@project_permission_required
def list_fascimile_collections(project):
    """
    List all available publicationFascimileCollections
    """
    pass


@de_tools.route("/<project>/fascimile_collection/link")
@project_permission_required
def link_fascimile_collection_to_publication(project):
    """
    Link a publicationFascimileCollection to a publication through publicationFascimile table
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list_links")
@project_permission_required
def list_fascimile_collection_links(project):
    """
    List all links between a publicationFascimileCollection and its publicationFascimile objects
    """
    pass


@de_tools.route("/projects/")
@jwt_required
def list_projects():
    """
    List all GDE projects
    """
    projects = Table("project", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([projects])
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result), 200


@de_tools.route("/<project>/publication_collection/list")
@project_permission_required
def list_publication_collections(project):
    """
    List all publicationCollection objects for a given project
    """
    pass


@de_tools.route("/<project>/publication_collection/new", methods=["POST"])
@project_permission_required
def new_publication_collection(project):
    """
    Create a new publicationCollection object and associated Introduction and Title objects.
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/")
@project_permission_required
def list_publications(project, collection_id):
    """
    List all publications in a given collection
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/<publication_id>")
@project_permission_required
def get_publication(project, collection_id, publication_id):
    """
    Get a publication object from the database
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/new", methods=["POST"])
@project_permission_required
def new_publication(project, collection_id):
    """
    Create a new publication object as part of the given publicationCollection
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/<publication_id>/link_file", methods=["POST"])
@project_permission_required
def link_file_to_publication(project, collection_id, publication_id):
    """
    Link an XML file to a publication,
    creating the appropriate publicationComment, publicationManuscript, or publicationVersion object.
    """
    pass
