from flask import Blueprint, jsonify, request, Response, safe_join
from flask_jwt_extended import get_jwt_identity, jwt_required, verify_jwt_in_request
from functools import wraps
import io
import logging
import mimetypes
import os
from ruamel.yaml import YAML
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
import subprocess
from werkzeug.utils import secure_filename

de_tools = Blueprint("digital_edition_tools", __name__)

metadata = MetaData()

logger = logging.getLogger("sls_api.de_tools")

# TODO new config for GDE_tools, since they're working with new database structures
# TODO git configuration?
# TODO branches?
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
    connection.close()
    return int(project_id["id"])


def select_all_from_table(table_name):
    table = Table(table_name, metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    rows = connection.execute(select([table])).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


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
    if "name" not in request_data:
        return jsonify({"msg": "No name in POST data"}), 400

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
            "msg": "Created new tag with ID {}".format(result.inserted_primary_key[0]),
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


@de_tools.route("/locations/")
@jwt_required
def get_locations():
    """
    Get all locations from the database
    """
    return select_all_from_table("location")


@de_tools.route("/subjects/")
@jwt_required
def get_subjects():
    """
    Get all subjects from the database
    """
    return select_all_from_table("subject")


@de_tools.route("/tags/")
@jwt_required
def get_tags():
    """
    Get all tags from the database
    """
    return select_all_from_table("tag")


@de_tools.route("/events/")
@jwt_required
def get_events():
    """
    Get a list of all available events in the database
    """
    return select_all_from_table("event")


@de_tools.route("/events/search", methods=["POST"])
@jwt_required
def find_event_by_description():
    """
    List all events whose description contains a given phrase

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    phrase: search-phrase for event description
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "phrase" not in request_data:
        return jsonify({"msg": "No phrase in POST data"}), 400

    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    statement = select([events]).where(events.c.description.like("%{}%".format(request_data["phrase"])))
    rows = connection.execute(statement).fetchall()

    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/events/new", methods=["POST"])
@jwt_required
def add_new_event():
    """
    Add a new event to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: event type
    description: event description
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()

    new_event = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
    }
    try:
        insert = events.insert()
        result = connection.execute(insert, **new_event)
        new_row = select([events]).where(events.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new event with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/event/<event_id>/connections/new", methods=["POST"])
@jwt_required
def connect_event(event_id):
    """
    Link an event to a location, subject, or tag through eventConnection

    POST data MUST be in JSON format.

    POST data MUST contain at least one of the following:
    subject_id: ID for the subject involved in the given event
    location_id: ID for the location involced in the given event
    tag_id: ID for the tag involved in the given event
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404
    event_connections = Table("eventConnection", metadata, autoload=True, autoload_with=db_engine)
    insert = event_connections.insert()
    new_event_connection = {
        "event_id": int(event_id),
        "subject_id": int(request_data["subject_id"]) if request_data.get("subject_id", None) else None,
        "location_id": int(request_data["location_id"]) if request_data.get("location_id", None) else None,
        "tag_id": int(request_data["tag_id"]) if request_data.get("tag_id", None) else None
    }
    try:
        result = connection.execute(insert, **new_event_connection)
        new_row = select([event_connections]).where(event_connections.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new eventConnection with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new eventConnection",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/event/<event_id>/connections")
@jwt_required
def get_event_connections(event_id):
    """
    List all eventConnections for a given event, to find related locations, subjects, and tags
    """
    event_connections = Table("eventConnection", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([event_connections]).where(event_connections.c.event_id == int(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/event/<event_id>/occurances")
@jwt_required
def get_event_occurances(event_id):
    """
    Get a list of all eventOccurances in the database, optionally limiting to a given event
    """
    event_occurances = Table("eventOccurance", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    statement = select([event_occurances]).where(event_occurances.c.event_id == int(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/event/<event_id>/occurances/new", methods=["POST"])
@jwt_required
def new_event_occurance(event_id):
    """
    Add a new eventOccurance to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    type: event occurance type
    description: event occurance description

    POST data SHOULD also contain at least one of the following:
    publication_id: ID for publication the event occurs in
    publicationVersion_id: ID for publication version the event occurs in
    publicationManuscript_id: ID for publication manuscript the event occurs in
    publicationFascimile_id: ID for publication fascimile the event occurs in
    publicationComment_id: ID for publication comment the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = Table("event", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404

    event_occurances = Table("eventOccurance", metadata, autoload=True, autoload_with=db_engine)
    insert = event_occurances.insert()
    new_occurance = {
        "event_id": int(event_id),
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "publication_id": int(request_data["publication_id"]) if request_data.get("publication_id", None) else None,
        "publicationVersion_id": int(request_data["publicationVersion_id"]) if request_data.get("publicationVersion_id", None) else None,
        "publicationManuscript_id": int(request_data["publicationManuscript_id"]) if request_data.get("publicationManuscript_id", None) else None,
        "publicationFascimile_id": int(request_data["publicationFascimile_id"]) if request_data.get("publicationFascimile_id", None) else None,
        "publicationComment_id": int(request_data["publicationComment_id"]) if request_data.get("publicationComment_id", None) else None,
    }
    try:
        result = connection.execute(insert, **new_occurance)
        new_row = select([event_occurances]).where(event_occurances.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new eventOccurance with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new eventOccurance",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/publications")
@jwt_required
def get_publications(project):
    """
    List all available publications in the given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    publication_collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publication_collections.c.id]).where(publication_collections.c.project_id == project_id)
    collection_ids = connection.execute(statement).fetchall()
    collection_ids = [int(row["id"]) for row in collection_ids]
    statement = select([publications]).where(publications.c.id.in_(collection_ids))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication/<publication_id>/versions")
@jwt_required
def get_publication_versions(project, publication_id):
    """
    List all versions of the given publication
    """
    connection = db_engine.connect()
    publication_versions = Table("publicationVersion", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publication_versions]).where(publication_versions.c.publication_id == int(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication/<publication_id>/manuscripts")
@jwt_required
def get_publication_manuscripts(project, publication_id):
    """
    List all manuscripts for the given publication
    """
    connection = db_engine.connect()
    publication_manuscripts = Table("publicationManuscript", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publication_manuscripts]).where(publication_manuscripts.c.publication_id == int(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication/<publication_id>/fascimiles")
@jwt_required
def get_publication_fascimiles(project, publication_id):
    """
    List all fascimilies for the given publication
    """
    connection = db_engine.connect()
    publication_fascimiles = Table("publicationFascimile", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publication_fascimiles]).where(publication_fascimiles.c.publication_id == int(publication_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication/<publication_id>/comments")
@jwt_required
def get_publication_comments(project, publication_id):
    """
    List all comments for the given publication
    """
    connection = db_engine.connect()
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    publication_comments = Table("publicationComment", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publications.c.publictionComment_id]).where(publications.c.id == int(publication_id))
    comment_ids = connection.execute(statement).fetchall()
    comment_ids = [int(row["id"]) for row in comment_ids]
    statement = select([publication_comments]).where(publication_comments.c.id.in_(comment_ids))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


def check_project_git_config(project):
    """
    Check the config file for project git repository configuration.
    Returns True if config okay, otherwise False and a message
    """
    if project not in config:
        return False, "Project config not found."
    if "git_repository" not in config[project]:
        return False, "git_repository not in project config."
    if "git_config" not in config[project]:
        return False, "git_config (SSH config) not in project config."
    if "git_branch" not in config[project]:
        return False, "git_branch information not in project config-"
    if "file_root" not in config[project]:
        return False, "file_root information not in project config."
    return True, "Project config OK."


def file_exists_in_git_root(project, file_path):
    """
    Check if the given file exists in the git repository for the given project
    Returns True if the file exists, otherwise False.
    """
    return os.path.exists(safe_join(config[project]["file_root"], file_path))


def run_git_command(project, command):
    """
    Helper method to run arbitrary git commands as if in the project's file_root folder
    @type project: str
    @type command: list
    """
    git_root = config[project]["file_root"]
    git_command = ["git", "-C", git_root, [c for c in command]]
    return subprocess.check_output(["git", "-C", git_root, git_command])


@de_tools.route("/<project>/sync_files_from_remote", methods=["POST"])
@project_permission_required
def pull_repository_changes_from_remote(project):
    """
    Sync API's local repo with the git remote, ensuring that all files are updated to their latest versions
    """
    # verify git config
    config_okay = check_project_git_config(project)
    if not config_okay[0]:
        return jsonify({"msg": config_okay[1]}), 500

    try:
        run_git_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git fetch failed to execute properly.",
            "reason": str(e.output)
        }), 500

    try:
        output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(config[project]["git_branch"])])
        new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git show failed to execute properly.",
            "reason": str(e.output)
        }), 500
    # merge in latest changes so that repository is updated
    try:
        run_git_command(project, ["merge", "origin/{}".format(config[project]["git_branch"])])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git merge failed to execute properly.",
            "reason": str(e.output)
        }), 500
    return jsonify({
        "msg": "Git repository successfully synced for project {}".format(project),
        "changed_files": new_and_changed_files
    })


@de_tools.route("/<project>/update_file/by_path/<path:file_path>", methods=["PUT"])
@project_permission_required
def update_file_in_remote(project, file_path):
    """
    Add new or update existing file in git remote.

    PUT data MUST be in JSON format

    PUT data MUST contain the following:
    xml_file: xml file to be created or updated in git repository

    PUT data MAY contain the following override information:
    author: email of the person authoring this change, if not given, JWT identity is used instead
    message: commit message for this change, if not given, generic "File update by <author>" message is used instead
    force: boolean value, if True uses force-push to override errors and possibly mangle the git remote to get the update through
    """
    # Check if request has valid JSON and set author/message/force accordingly
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No JSON data in PUT request."}), 400
    elif "xml_file" not in request.files:
        return jsonify({"msg": "No xml_file in PUT request."}), 400
    else:
        author = request_data.get("author", get_jwt_identity()["sub"])
        message = request_data.get("message", "File update by {}".format(author))
        force = bool(request_data.get("force", False))

        xml_file = request.files["xml_file"]

    # verify git config
    config_okay = check_project_git_config(project)
    if not config_okay[0]:
        return jsonify({"msg": config_okay[1]}), 500

    # fetch latest changes from remote
    try:
        run_git_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git fetch failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # check if desired file has changed in remote since last update
    # if so, fail and return both user file and repo file to user, unless force=True
    try:
        output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(config[project]["git_branch"])])
        new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git show failed to execute properly.",
            "reason": str(e.output)
        }), 500
    if safe_join(config[project]["file_root"], file_path) in new_and_changed_files and not force:
        with io.open(safe_join(config[project]["file_root"], file_path), encoding="UTF-8", mode="rb") as repo_file:
            return jsonify({
                "msg": "File {} has been changed in git repository since last update, please manually check file changes.",
                "your_file": xml_file,
                "repo_file": repo_file.read()
            }), 400

    # merge in latest changes so that the local repository is updated
    try:
        run_git_command(project, ["merge",  "origin/{}".format(config[project]["git_branch"])])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git merge failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # check the status of the git repo, so we know if we need to git add later
    file_exists = file_exists_in_git_root(project, file_path)

    # Secure filename and save new file to local repo
    filename = secure_filename(xml_file.filename)
    if xml_file and filename:
        xml_file.save(os.path.join(config[project]["file_root"], filename))

    # Add/commit file to local repo and push to remote
    if not file_exists:
        # TODO git add
        pass

    # TODO git commit
    # TODO git push


@de_tools.route("/<project>/get_latest_file/by_path/<path:file_path>")
@project_permission_required
def get_file_from_remote(project, file_path):
    """
    Get latest XML file from git remote
    """
    config_okay = check_project_git_config(project)
    if not config_okay[0]:
        return jsonify({"msg": config_okay[1]}), 500

    # git fetch && git checkout origin/<branch> -- file_path
    try:
        run_git_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git fetch failed to execute properly.",
            "reason": str(e.output)
        }), 500
    try:
        run_git_command(project, ["checkout", "origin/{}".format(config[project]["git_branch"]), "--", file_path])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git checkout failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # This will download latest changes for this branch, but only update the file we're interested in in the local repo
    # This way, we don't have to wait for other file updates if there are lots of changes in the repo

    if file_exists_in_git_root(project, file_path):
        with io.open(safe_join(config[project]["file_root"], file_path), "rb") as file:
            output = io.BytesIO()
            output.write(file.read())
            content = output.getvalue()
            output.close()
            mimetype = mimetypes.guess_type(safe_join(config[project]["file_root"], file_path))[0]
            if mimetype is None:
                mimetype = "application/octet-stream"  # if unable to guess filetype, mark as arbitrary binary data and let user sort it out
            return Response(content, 200, mimetype=mimetype, content_type=mimetype)
    else:
        return jsonify({"msg": "The requested file was not found in the git repository."}), 404


@de_tools.route("/<project>/get_tree/")
@de_tools.route("/<project>/get_tree/<path:file_path>")
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

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    title: collection type
    description: collection description
    folderPath: path to fascimiles for this collection

    POST data MAY also contain:
    numberOfPages: total number of pages in this collection
    startPageNumber: number for starting page of this collection
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    collections = Table("publicationFascimileCollection", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    insert = collections.insert()

    new_collection = {
        "title": request_data.get("title", None),
        "description": request_data.get("description", None),
        "folderPath": request_data.get("folderPath", None),
        "numberOfPages": request_data.get("numberOfPages", None),
        "startPageNumber": request_data.get("startPageNumber", None)
    }
    try:
        result = connection.execute(insert, **new_collection)
        new_row = select([collections]).where(collections.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new publicationFascimileCollection with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publicationFascimileCollection",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/fascimile_collection/list")
@project_permission_required
def list_fascimile_collections(project):
    """
    List all available publicationFascimileCollections
    """
    return select_all_from_table("publicationFascimileCollections")


@de_tools.route("/<project>/fascimile_collection/<collection_id>/link", methods=["POST"])
@project_permission_required
def link_fascimile_collection_to_publication(project, collection_id):
    """
    Link a publicationFascimileCollection to a publication through publicationFascimile table

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    publication_id: ID for the publication to link to

    POST data MAY also contain the following:
    publicationManuscript_id: ID for the specific publication manuscript to link to
    publicationVersion_id: ID for the specific publication version to link to
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "publication_id" not in request_data:
        return jsonify({"msg": "No publication_id in POST data."}), 400

    connection = db_engine.connect()
    publication_id = int(request_data["publication_id"])
    project_id = get_project_id_from_name(project)

    publication_fascimiles = Table("publicationFascimile", metadata, autoload=True, autoload_with=db_engine)
    publication_collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)

    statement = select([publications.c.publicationCollection_id]).where(publications.c.id == publication_id)
    result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404
    publication_collection_id = int(result[0]["publicationCollection_id"])

    statement = select([publication_collections.c.project_id]).where(publication_collections.c.id == publication_collection_id)
    result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404

    if result[0]["project_id"] != project_id:
        return jsonify(
            {
                "msg": "Publication {} appears to not belong to project {!r}".format(publication_id, project)
            }
        ), 400

    insert = publication_fascimiles.insert()
    new_fascimile = {
        "publicationFascimileCollection_id": collection_id,
        "publication_id": publication_id,
        "publicationManuscript_id": request_data.get("publicationManuscript_id", None),
        "publicationVersion_id": request_data.get("publicationVersion_id", None)
    }
    try:
        result = connection.execute(insert, **new_fascimile)
        new_row = select([publication_fascimiles]).where(publication_fascimiles.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new publicationFascimile with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publicationFascimile",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/fascimile_collection/<collection_id>/list_links")
@project_permission_required
def list_fascimile_collection_links(project, collection_id):
    """
    List all publicationFascimile objects in the given publicationFascimileCollection
    """
    connection = db_engine.connect()
    fascimiles = Table("publicationFascimile", metadata, autoload=True, autoload_with=db_engine)
    statement = select([fascimiles]).where(fascimiles.c.publicationFascimileCollection_id == int(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/projects/")
@jwt_required
def get_projects():
    """
    List all GDE projects
    """
    return select_all_from_table("project")


@de_tools.route("/<project>/publication_collection/list")
@project_permission_required
def list_publication_collections(project):
    """
    List all publicationCollection objects for a given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    statement = select([collections]).where(collections.c.project_id == int(project_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication_collection/new", methods=["POST"])
@project_permission_required
def new_publication_collection(project):
    """
    Create a new publicationCollection object and associated Introduction and Title objects.
    """
    pass


@de_tools.route("/<project>/publication_collection/<collection_id>/publications")
@project_permission_required
def list_publications(project, collection_id):
    """
    List all publications in a given collection
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    statement = select([collections]).where(collections.c.id == int(collection_id))
    rows = connection.execute(statement).fetchall()
    if len(rows) != 1:
        return jsonify(
            {
                "msg": "Could not find collection in database."
            }
        ), 404
    elif rows[0]["project_id"] != int(project_id):
        return jsonify(
            {
                "msg": "Found collection not part of {!r} with ID {}.".format(project, project_id)
            }
        ), 400
    statement = select([publications]).where(publications.c.publicationCollection_id == int(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@de_tools.route("/<project>/publication/<publication_id>")
@project_permission_required
def get_publication(project, publication_id):
    """
    Get a publication object from the database
    """
    connection = db_engine.connect()
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    statement = select([publications]).where(publications.c.id == int(publication_id))
    rows = connection.execute(statement).fetchall()
    result = dict(rows[0])
    return jsonify(result)


@de_tools.route("/<project>/publication_collection/<collection_id>/publications/new", methods=["POST"])
@project_permission_required
def new_publication(project, collection_id):
    """
    Create a new publication object as part of the given publicationCollection

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    name: publication name

    POST data MAY also contain the following:
    publicationInformation_id: ID for related publicationInformation object
    publicationComment_id: ID for related publicationComment object
    datePublishedExternally: date of external publication for publication
    published: publish status for publication
    legacyId: legacy ID for publication
    publishedBy: person responsible for publishing the publication
    originalFilename: filepath to publication XML file
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    project_id = get_project_id_from_name(project)

    connection = db_engine.connect()
    collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)

    statement = select([collections.c.project_id]).where(collections.c.id == int(collection_id))
    result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "publicationCollection not found."
            }
        ), 404

    if result[0]["project_id"] != project_id:
        return jsonify(
            {
                "msg": "publicationCollection {} does not belong to project {!r}".format(collection_id, project)
            }
        ), 400

    insert = publications.insert()

    publication = {
        "name": request_data.get("name", None),
        "publicationInformation_id": request_data.get("publicationInformation_id", None),
        "publicationComment_id": request_data.get("publicationComment_id", None),
        "datePublishedExternally": request_data.get("datePublishedExternally", None),
        "published": request_data.get("published", None),
        "legacyId": request_data.get("legacyId", None),
        "publishedBy": request_data.get("publishedBy", None),
        "originalFilename": request_data.get("originalFileName", None)
    }
    try:
        result = connection.execute(insert, **publication)
        new_row = select([publications]).where(publications.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new publication with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publication",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@de_tools.route("/<project>/publication/<publication_id>/link_file", methods=["POST"])
@project_permission_required
def link_file_to_publication(project, publication_id):
    """
    Link an XML file to a publication,
    creating the appropriate publicationComment, publicationManuscript, or publicationVersion object.

    POST data MUST be in JSON format

    POST data MUST contain the following:
    file_path: path to the file to be linked
    """
    pass
