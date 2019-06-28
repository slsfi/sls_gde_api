import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import db_engine, int_or_none, metadata, project_permission_required


publishing_tools = Blueprint("publishing_tools", __name__)

logger = logging.getLogger("sls_api.publishing_tools")


@publishing_tools.route("/projects/new/", methods=["POST"])
@jwt_required
def add_new_project():
    """
    Takes project name as JSON data
    Returns "msg" and "project_id" on success
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    name = request_data.get("name", None)

    projects = Table("project", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()
    ins = projects.insert().values(name=name)

    result = connection.execute(ins)
    connection.close()
    return jsonify({
        "msg": "Created new project.",
        "project_id": int(result.inserted_primary_key[0])
    }), 201


@publishing_tools.route("/projects/<project_id>/edit/", methods=["POST"])
@jwt_required
def edit_project(project_id):
    """
    Takes "name" and/or "published" as JSON data
    Returns "msg" and "project_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    name = request_data.get("name", None)
    published = request_data.get("published", None)

    projects = Table("project", metadata, autoload=True, autoload_with=db_engine)
    query = select([projects.c.id]).where(projects.c.id == int_or_none(project_id))
    connection = db_engine.connect()

    result = connection.execute(query)
    if len(result.fetchall()) != 1:
        connection.close()
        return jsonify("No such project exists."), 404

    values = {}
    if name is not None:
        values["name"] = name
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        update = projects.update().where(projects.c.id == int(project_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated project {} with values {}".format(project_id, str(values)),
            "project_id": project_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication_collection/<collection_id>/edit/", methods=["POST"])
@project_permission_required
def edit_publication_collection(project, collection_id):
    """
    Takes "name" and/or "published" as JSON data
    Returns "msg" and "publication_collection_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    name = request_data.get("name", None)
    published = request_data.get("published", None)

    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.id]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()

    result = connection.execute(query)
    if len(result.fetchall()) != 1:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if name is not None:
        values["name"] = name
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        update = collections.update().where(collections.c.id == int(collection_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated publication collection {} with values {}".format(collection_id, str(values)),
            "publication_collection_id": collection_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/")
@project_permission_required
def get_intro(project, collection_id):
    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    introductions = Table("publication_collection_introduction", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_introduction_id]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query)
    if result.fetchone() is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    query = select([introductions])\
        .where(introductions.c.id == int(result[collections.c.publication_collection_introduction_id]))

    row = dict(connection.execute(query).fetchone())
    connection.close()
    return jsonify(row)


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/edit/", methods=["POST"])
@project_permission_required
def edit_intro(project, collection_id):
    """
    Takes "filename" and/or "published" as JSON data
    Returns "msg" and "introduction_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    introductions = Table("publication_collection_introduction", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_introduction_id]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        intro_id = int(result[0])
        update = introductions.update().where(introductions.c.id == intro_id).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated publication collection introduction {} with values {}".format(intro_id, str(values)),
            "introduction_id": intro_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/")
@project_permission_required
def get_title(project, collection_id):
    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    titles = Table("publication_collection_title", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_title_id]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    query = select([titles]).where(titles.c.id == int(result[collections.c.publication_collection_title_id]))

    row = dict(connection.execute(query).fetchone())
    connection.close()
    return jsonify(row)


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/edit/", methods=["POST"])
@project_permission_required
def edit_title(project, collection_id):
    """
    Takes "filename" and/or "published" as JSON data
    Returns "msg" and "title_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    titles = Table("publication_collection_title", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_title_id]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        title_id = int(result[0])
        update = titles.update().where(titles.c.id == title_id).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated publication collection title {} with values {}".format(title_id, str(values)),
            "title_id": title_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/edit/", methods=["POST"])
@project_permission_required
def edit_publication(project, publication_id):
    """
    Takes "title", "genre", "filename", "published" as JSON data
    Returns "msg" and "publication_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    genre = request_data.get("genre", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications.c.id]).where(publications.c.id == int_or_none(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query)
    if len(result.fetchall()) != 1:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if genre is not None:
        values["genre"] = genre
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        update = publications.update().where(publications.c.id == int(publication_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated project {} with values {}".format(publication_id, str(values)),
            "publication_id": publication_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/comment/edit/", methods=["POST"])
@project_permission_required
def edit_comment(project, publication_id):
    """
    Takes "filename" and/or "published" as JSON data
    If there is no publication_comment in the database, creates one
    Returns "msg" and "comment_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    comments = Table("publication_comment", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications.c.publication_comment_id]).where(publications.c.id == int_or_none(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    comment_id = result[0]

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    if len(values) > 0:
        if comment_id is not None:
            update = comments.update().where(comments.c.id == int(comment_id)).values(**values)
            connection.execute(update)
            connection.close()
            return jsonify({
                "msg": "Updated comment {} with values {}".format(comment_id, str(values)),
                "comment_id": comment_id
            })
        else:
            insert = comments.insert().values(**values)
            r = connection.execute(insert)
            comment_id = r.inserted_primary_key[0]
            update = publications.update().where(publications.c.id == int(publication_id)).values({"publication_comment_id": int(comment_id)})
            connection.execute(update)
            connection.close()
            return jsonify({
                "msg": "Created comment {} for publication {} with values {}".format(r.inserted_primary_key, publication_id[0], str(values)),
                "comment_id": r.inserted_primary_key[0]
            })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/manuscripts/new/", methods=["POST"])
@project_permission_required
def add_manuscript(project, publication_id):
    """
    Takes "title", "filename", "published", "sort_order" as JSON data
    Returns "msg" and "manuscript_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)
    sort_order = request_data.get("sort_order", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    manuscripts = Table("publication_manuscript", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications]).where(publications.c.id == int_or_none(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {"publication_id": int(publication_id)}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published
    if sort_order is not None:
        values["sort_order"] = sort_order

    insert = manuscripts.insert().values(**values)
    result = connection.execute(insert)
    return jsonify({
        "msg": "Created new manuscript object.",
        "manuscript_id": int(result.inserted_primary_key[0])
    }), 201


@publishing_tools.route("/<project>/manuscripts/<manuscript_id>/edit/", methods=["POST"])
@project_permission_required
def edit_manuscript(project, manuscript_id):
    """
    Takes "title", "filename", "published", "sort_order" as JSON data
    Returns "msg" and "manuscript_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)
    sort_order = request_data.get("sort_order", None)

    manuscripts = Table("publication_manuscript", metadata, autoload=True, autoload_with=db_engine)
    query = select([manuscripts]).where(manuscripts.c.id == int_or_none(manuscript_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such manuscript exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published
    if sort_order is not None:
        values["sort_order"] = sort_order

    if len(values) > 0:
        update = manuscripts.update().where(manuscripts.c.id == int(manuscript_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated manuscript {} with values {}".format(int(manuscript_id), str(values)),
            "manuscript_id": int(manuscript_id)
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/versions/new/", methods=["POST"])
@project_permission_required
def add_version(project, publication_id):
    """
    Takes "title", "filename", "published", "sort_order", "type" as JSON data
    "type" denotes version type, 1=base text, 2=other variant
    Returns "msg" and "version_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)
    sort_order = request_data.get("sort_order", None)
    version_type = request_data.get("type", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    versions = Table("publication_version", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications]).where(publications.c.id == int_or_none(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {"publication_id": int(publication_id)}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published
    if sort_order is not None:
        values["sort_order"] = sort_order
    if version_type is not None:
        values["type"] = version_type

    insert = versions.insert().values(**values)
    result = connection.execute(insert)
    return jsonify({
        "msg": "Created new version object.",
        "version_id": int(result.inserted_primary_key[0])
    }), 201


@publishing_tools.route("/<project>/versions/<version_id>/edit/", methods=["POST"])
@project_permission_required
def edit_version(project, version_id):
    """
    Takes "title", "filename", "published", "sort_order", "type" as JSON data
    "type" denotes version type, 1=base text, 2=other variant
    Returns "msg" and "version_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)
    sort_order = request_data.get("sort_order", None)
    version_type = request_data.get("type", None)

    versions = Table("publication_version", metadata, autoload=True, autoload_with=db_engine)
    query = select([versions]).where(versions.c.id == int_or_none(version_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such version exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published
    if sort_order is not None:
        values["sort_order"] = sort_order
    if version_type is not None:
        values["type"] = version_type

    if len(values) > 0:
        update = versions.update().where(versions.c.id == int(version_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated version {} with values {}".format(int(version_id), str(values)),
            "manuscript_id": int(version_id)
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/facsimile_collection/<collection_id>/edit/", methods=["POST"])
@project_permission_required
def edit_facsimile_collection(project, collection_id):
    """
    Takes "title", "numberOfPages", "startPageNumber", "description" as JSON data
    Returns "msg" and "facs_coll_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    page_count = request_data.get("numberOfPages", None)
    start_page = request_data.get("startPageNumber", None)
    description = request_data.get("description", None)

    collections = Table("publication_facsimile_collection", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such facsimile collection exists."), 404

    values = {}
    if title is not None:
        values["title"] = title
    if page_count is not None:
        values["number_of_pages"] = page_count
    if start_page is not None:
        values["start_page_number"] = start_page
    if description is not None:
        values["description"] = description

    if len(values) > 0:
        update = collections.update().where(collections.c.id == int(collection_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated facsimile collection {} with values {}".format(int(collection_id), str(values)),
            "facs_coll_id": int(collection_id)
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication_collection/<collection_id>/info")
@project_permission_required
def get_publication_collection_info(project, collection_id):
    """
    Returns published status for publication_collection and associated introduction and title objects
    Also returns the original_filename for the introduction and title objects
    """
    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    intros = Table("publication_collection_introduction", metadata, autoload=True, autoload_with=db_engine)
    titles = Table("publication_collection_title", metadata, autoload=True, autoload_with=db_engine)

    query = select([collections]).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    collection_result = connection.execute(query).fetchone()
    if collection_result is None:
        connection.close()
        return jsonify("No such publication collection exists"), 404

    intro_id = int_or_none(collection_result["publication_collection_introduction_id"])
    title_id = int_or_none(collection_result["publication_collection_title_id"])
    intro_query = select([intros.c.published, intros.c.original_filename]).where(intros.c.id == intro_id)
    title_query = select([titles.c.published, titles.c.original_filename]).where(titles.c.id == title_id)

    intro_result = connection.execute(intro_query).fetchone()
    title_result = connection.execute(title_query).fetchone()

    connection.close()
    result = {
        "collection_id": int(collection_id),
        "collection_published": collection_result["published"],
        "intro_id": intro_id,
        "intro_published": None if intro_result is None else intro_result["published"],
        "intro_original_filename": None if intro_result is None else intro_result["original_filename"],
        "title_id": title_id,
        "title_published": None if title_result is None else title_result["published"],
        "title_original_filename": None if title_result is None else title_result["original_filename"]
    }
    return jsonify(result)


@publishing_tools.route("/<project>/locations/<location_id>/edit", methods=["POST"])
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

    locations = Table("location", metadata, autoload=True, autoload_with=db_engine)

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
        update = locations.update().where(locations.c.id == int(location_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated location {} with values {}".format(int(location_id), str(values)),
            "location_id": int(location_id)
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/subjects/<subject_id>/edit", methods=["POST"])
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

    subjects = Table("subject", metadata, autoload=True, autoload_with=db_engine)

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
        update = subjects.update().where(subjects.c.id == int(subject_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated subject {} with values {}".format(int(subject_id), str(values)),
            "subject_id": int(subject_id)
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400
