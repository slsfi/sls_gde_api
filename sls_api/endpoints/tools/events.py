from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import cast, select, Text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, select_all_from_table

event_tools = Blueprint("event_tools", __name__)


@event_tools.route("/<project>/locations/new/", methods=["POST"])
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
    legacy_id: legacy id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "name" not in request_data:
        return jsonify({"msg": "No name in POST data"}), 400

    locations = get_table("location")
    connection = db_engine.connect()

    new_location = {
        "name": request_data["name"],
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "legacy_id": request_data.get("legacy_id", None),
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


@event_tools.route("/<project>/locations/<location_id>/edit/", methods=["POST"])
@project_permission_required
def edit_location(project, location_id):
    """
    Edit a location object in the database

    POST data MUST be in JSON format.

    POST data CAN contain:
    name: location name
    description: location description
    legacy_id: legacy id for location
    latitude: latitude coordinate for location
    longitude: longitude coordinate for location
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    locations = get_table("location")

    connection = db_engine.connect()
    location_query = select([locations.c.id]).where(locations.c.id == int_or_none(location_id))
    location_row = connection.execute(location_query).fetchone()
    if location_row is None:
        return jsonify({"msg": "No location with an ID of {} exists.".format(location_id)}), 404

    name = request_data.get("name", None)
    description = request_data.get("description", None)
    legacy_id = request_data.get("legacy_id", None)
    latitude = request_data.get("latitude", None)
    longitude = request_data.get("longitude", None)

    values = {}
    if name is not None:
        values["name"] = name
    if description is not None:
        values["description"] = description
    if legacy_id is not None:
        values["legacy_id"] = legacy_id
    if latitude is not None:
        values["latitude"] = latitude
    if longitude is not None:
        values["longitude"] = longitude

    if len(values) > 0:
        try:
            update = locations.update().where(locations.c.id == int(location_id)).values(**values)
            connection.execute(update)
            return jsonify({
                "msg": "Updated location {} with values {}".format(int(location_id), str(values)),
                "location_id": int(location_id)
            })
        except Exception as e:
            result = {
                "msg": "Failed to update location.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/<project>/subjects/new/", methods=["POST"])
@project_permission_required
def add_new_subject(project):
    """
    Add a new subject object to the database

    POST data MUST be in JSON format

    POST data SHOULD contain:
    type: subject type
    description: subject description

    POST data CAN also contain:
    first_name: Subject first or given name
    last_name Subject surname
    preposition: preposition for subject
    full_name: Subject full name
    legacy_id: Legacy id for subject
    date_born: Subject date of birth
    date_deceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    subjects = get_table("subject")
    connection = db_engine.connect()

    new_subject = {
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "project_id": get_project_id_from_name(project),
        "first_name": request_data.get("first_name", None),
        "last_name": request_data.get("last_name", None),
        "preposition": request_data.get("preposition", None),
        "full_name": request_data.get("full_name", None),
        "legacy_id": request_data.get("legacy_id", None),
        "date_born": request_data.get("date_born", None),
        "date_deceased": request_data.get("date_deceased", None)
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


@event_tools.route("/<project>/subjects/<subject_id>/edit/", methods=["POST"])
@project_permission_required
def edit_subject(project, subject_id):
    """
    Edit a subject object in the database

    POST data MUST be in JSON format

    POST data CAN contain:
    type: subject type
    description: subject description
    first_name: Subject first or given name
    last_name: Subject surname
    preposition: preposition for subject
    full_name: Subject full name
    legacy_id: Legacy id for subject
    date_born: Subject date of birth
    date_deceased: Subject date of death
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    subjects = get_table("subject")

    connection = db_engine.connect()
    subject_query = select([subjects.c.id]).where(subjects.c.id == int_or_none(subject_id))
    subject_row = connection.execute(subject_query).fetchone()
    if subject_row is None:
        return jsonify({"msg": "No subject with an ID of {} exists.".format(subject_id)}), 404

    subject_type = request_data.get("type", None)
    description = request_data.get("description", None)
    first_name = request_data.get("first_name", None)
    last_name = request_data.get("last_name", None)
    preposition = request_data.get("preposition", None)
    full_name = request_data.get("full_name", None)
    legacy_id = request_data.get("legacy_id", None)
    date_born = request_data.get("date_born", None)
    date_deceased = request_data.get("date_deceased", None)

    values = {}
    if subject_type is not None:
        values["type"] = subject_type
    if description is not None:
        values["description"] = description
    if first_name is not None:
        values["first_name"] = first_name
    if last_name is not None:
        values["last_name"] = last_name
    if preposition is not None:
        values["preposition"] = preposition
    if full_name is not None:
        values["full_name"] = full_name
    if legacy_id is not None:
        values["legacy_id"] = legacy_id
    if date_born is not None:
        values["date_born"] = date_born
    if date_deceased is not None:
        values["date_deceased"] = date_deceased

    if len(values) > 0:
        try:
            update = subjects.update().where(subjects.c.id == int(subject_id)).values(**values)
            connection.execute(update)
            return jsonify({
                "msg": "Updated subject {} with values {}".format(int(subject_id), str(values)),
                "subject_id": int(subject_id)
            })
        except Exception as e:
            result = {
                "msg": "Failed to update subject.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/<project>/tags/new/", methods=["POST"])
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
    legacy_id: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    tags = get_table("tag")
    connection = db_engine.connect()

    new_tag = {
        "type": request_data.get("type", None),
        "name": request_data.get("name", None),
        "project_id": get_project_id_from_name(project),
        "description": request_data.get("description", None),
        "legacy_id": request_data.get("legacy_id", None)
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


@event_tools.route("/<project>/tags/<tag_id>/edit/", methods=["POST"])
@project_permission_required
def edit_tag(project, tag_id):
    """
    Update tag object to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    type: tag type
    name: tag name

    POST data CAN also contain:
    description: tag description
    legacy_id: Legacy id for tag
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    tags = get_table("tag")

    connection = db_engine.connect()
    tag_query = select([tags.c.id]).where(tags.c.id == int_or_none(tag_id))
    tag_row = connection.execute(tag_query).fetchone()
    if tag_row is None:
        return jsonify({"msg": "No tag with an ID of {} exists.".format(tag_id)}), 404

    type = request_data.get("type", None)
    name = request_data.get("name", None)
    description = request_data.get("description", None)
    legacy_id = request_data.get("legacy_id", None)

    values = {}
    if type is not None:
        values["type"] = type
    if name is not None:
        values["name"] = name
    if description is not None:
        values["description"] = description
    if legacy_id is not None:
        values["legacy_id"] = legacy_id

    if len(values) > 0:
        try:
            update = tags.update().where(tags.c.id == int(tag_id)).values(**values)
            connection.execute(update)
            return jsonify({
                "msg": "Updated tag {} with values {}".format(int(tag_id), str(values)),
                "tag_id": int(tag_id)
            })
        except Exception as e:
            result = {
                "msg": "Failed to update tag.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@event_tools.route("/locations/")
@jwt_required
def get_locations():
    """
    Get all locations from the database
    """
    return select_all_from_table("location")


@event_tools.route("/subjects/")
@jwt_required
def get_subjects():
    """
    Get all subjects from the database
    """
    connection = db_engine.connect()
    subject = get_table("subject")
    columns = [subject.c.id, [cast(subject.c.date_created, Text), subject.c.date_created.label('date_created')],
               [cast(subject.c.date_modified, Text), subject.c.date_modified.label('date_modified')],
               subject.c.deleted, subject.c.type, subject.c.first_name, subject.c.last_name,
               subject.c.place_of_birth, subject.c.occupation, subject.c.preposition,
               subject.c.full_name, subject.c.description, subject.c.legacy_id,
               [cast(subject.c.date_born, Text), subject.c.date_born.label('date_born')],
               [cast(subject.c.date_deceased, Text), subject.c.date_deceased.label('date_deceased')],
               subject.c.project_id, subject.c.source]
    stmt = select(columns)
    rows = connection.execute(stmt).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/tags/")
@jwt_required
def get_tags():
    """
    Get all tags from the database
    """
    return select_all_from_table("tag")


@event_tools.route("/events/")
@jwt_required
def get_events():
    """
    Get a list of all available events in the database
    """
    return select_all_from_table("event")


@event_tools.route("/events/search/", methods=["POST"])
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

    events = get_table("event")
    connection = db_engine.connect()

    statement = select([events]).where(events.c.description.ilike("%{}%".format(request_data["phrase"])))
    rows = connection.execute(statement).fetchall()

    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/events/new/", methods=["POST"])
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
    events = get_table("event")
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


@event_tools.route("/event/<event_id>/connections/new/", methods=["POST"])
@jwt_required
def connect_event(event_id):
    """
    Link an event to a location, subject, or tag through event_connection

    POST data MUST be in JSON format.

    POST data MUST contain at least one of the following:
    subject_id: ID for the subject involved in the given event
    location_id: ID for the location involved in the given event
    tag_id: ID for the tag involved in the given event
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = get_table("event")
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int_or_none(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404
    event_connections = get_table("event_connection")
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
            "msg": "Created new event_connection with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event_connection",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@event_tools.route("/event/<event_id>/connections/")
@jwt_required
def get_event_connections(event_id):
    """
    List all event_connections for a given event, to find related locations, subjects, and tags
    """
    event_connections = get_table("event_connection")
    connection = db_engine.connect()
    statement = select([event_connections]).where(event_connections.c.event_id == int_or_none(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/")
@jwt_required
def get_event_occurrences(event_id):
    """
    Get a list of all event_occurrence in the database, optionally limiting to a given event
    """
    event_occurrences = get_table("event_occurrence")
    connection = db_engine.connect()
    statement = select([event_occurrences]).where(event_occurrences.c.event_id == int_or_none(event_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@event_tools.route("/event/<event_id>/occurrences/new/", methods=["POST"])
@jwt_required
def new_event_occurrence(event_id):
    """
    Add a new event_occurrence to the database

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    type: event occurrence type
    description: event occurrence description

    POST data SHOULD also contain at least one of the following:
    publication_id: ID for publication the event occurs in
    publicationVersion_id: ID for publication version the event occurs in
    publicationManuscript_id: ID for publication manuscript the event occurs in
    publicationFacsimile_id: ID for publication facsimile the event occurs in
    publicationComment_id: ID for publication comment the event occurs in
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    events = get_table("event")
    connection = db_engine.connect()
    select_event = select([events]).where(events.c.id == int_or_none(event_id))
    event_exists = connection.execute(select_event).fetchall()
    if len(event_exists) != 1:
        return jsonify(
            {
                "msg": "Event ID not found in database"
            }
        ), 404

    event_occurrences = get_table("event_occurrence")
    insert = event_occurrences.insert()
    new_occurrence = {
        "event_id": int(event_id),
        "type": request_data.get("type", None),
        "description": request_data.get("description", None),
        "publication_id": int(request_data["publication_id"]) if request_data.get("publication_id", None) else None,
        "publication_version_id": int(request_data["publicationVersion_id"]) if request_data.get("publicationVersion_id", None) else None,
        "publication_manuscript_id": int(request_data["publicationManuscript_id"]) if request_data.get("publicationManuscript_id", None) else None,
        "publication_facsimile_id": int(request_data["publicationFacsimile_id"]) if request_data.get("publicationFacsimile_id", None) else None,
        "publication_comment_id": int(request_data["publicationComment_id"]) if request_data.get("publicationComment_id", None) else None,
    }
    try:
        result = connection.execute(insert, **new_occurrence)
        new_row = select([event_occurrences]).where(event_occurrences.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new event_occurrence with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new event_occurrence",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()
