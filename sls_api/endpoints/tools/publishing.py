import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import select
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_table, int_or_none, project_permission_required


publishing_tools = Blueprint("publishing_tools", __name__)

logger = logging.getLogger("sls_api.tools.publishing")


@publishing_tools.route("/projects/new/", methods=["POST"])
@jwt_required()
def add_new_project():
    """
    Takes project name as JSON data
    Returns "msg" and "project_id" on success
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    name = request_data.get("name", None)

    projects = get_table("project")
    connection = db_engine.connect()
    ins = projects.insert().values(name=name)

    result = connection.execute(ins)
    connection.close()
    return jsonify({
        "msg": "Created new project.",
        "project_id": int(result.inserted_primary_key[0])
    }), 201


@publishing_tools.route("/projects/<project_id>/edit/", methods=["POST"])
@jwt_required()
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

    projects = get_table("project")
    query = select(projects.c.id).where(projects.c.id == int_or_none(project_id))
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

    values["date_modified"] = datetime.now()

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

    collection_title_id = request_data.get("publication_collection_title_id", None)
    collection_title_filename = request_data.get("collection_title_filename", None)
    collection_intro_id = request_data.get("publication_collection_introduction_id", None)
    collection_intro_filename = request_data.get("collection_intro_filename", None)

    collections = get_table("publication_collection")
    query = select(collections.c.id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()

    result = connection.execute(query)
    if len(result.fetchall()) != 1:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    introductions = get_table("publication_collection_introduction")
    titles = get_table("publication_collection_title")

    new_intro = {
        "published": request_data.get("intro_published", 1),
        "original_filename": request_data.get("collection_intro_filename", 1)
    }

    new_title = {
        "published": request_data.get("title_published", 1),
        "original_filename": request_data.get("collection_title_filename", 1)
    }

    if collection_title_id is None and collection_title_filename is not None:
        # Create a new title and add the id to the Collection
        ins = titles.insert()
        result = connection.execute(ins, **new_title)
        new_title_row = select(titles).where(titles.c.id == result.inserted_primary_key[0])
        new_title_row = connection.execute(new_title_row).fetchone()
        new_title_row = new_title_row._asdict()
        collection_title_id = new_title_row["id"]

    if collection_intro_id is None and collection_intro_filename is not None:
        # Create a new intro and add the id to the Collection
        ins = introductions.insert()
        result = connection.execute(ins, **new_intro)
        new_intro_row = select(introductions).where(introductions.c.id == result.inserted_primary_key[0])
        new_intro_row = connection.execute(new_intro_row).fetchone()
        new_intro_row = new_intro_row._asdict()
        collection_intro_id = new_intro_row["id"]

    if collection_title_id is not None:
        # Update the Title data
        update = introductions.update().where(titles.c.id == collection_title_id).values(**new_title)
        connection.execute(update)

    if collection_intro_id is not None:
        # Update the Intro data
        update = introductions.update().where(introductions.c.id == collection_intro_id).values(**new_intro)
        connection.execute(update)

    values = {}
    if name is not None:
        values["name"] = name
    if published is not None:
        values["published"] = published
    if collection_intro_id is not None:
        values["publication_collection_introduction_id"] = collection_intro_id
    if collection_title_id is not None:
        values["publication_collection_title_id"] = collection_title_id

    values["date_modified"] = datetime.now()

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
    collections = get_table("publication_collection")
    introductions = get_table("publication_collection_introduction")
    query = select(collections.c.publication_collection_introduction_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query)
    if result.fetchone() is None:
        result.close()
        return jsonify("No such publication collection exists."), 404

    query = select(introductions).where(introductions.c.id == int(result[collections.c.publication_collection_introduction_id]))

    row = connection.execute(query).fetchone()
    if row is not None:
        row = row._asdict()
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

    collections = get_table("publication_collection")
    introductions = get_table("publication_collection_introduction")
    query = select(collections.c.publication_collection_introduction_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    values["date_modified"] = datetime.now()

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
    collections = get_table("publication_collection")
    titles = get_table("publication_collection_title")
    query = select(collections.c.publication_collection_title_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    query = select(titles).where(titles.c.id == int(result[collections.c.publication_collection_title_id]))

    row = connection.execute(query).fetchone()
    if row is not None:
        row = row._asdict()
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

    collections = get_table("publication_collection")
    titles = get_table("publication_collection_title")
    query = select(collections.c.publication_collection_title_id).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication collection exists."), 404

    values = {}
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

    values["date_modified"] = datetime.now()

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

    publications = get_table("publication")
    query = select(publications.c.id).where(publications.c.id == int_or_none(publication_id))
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

    values["date_modified"] = datetime.now()

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

    publications = get_table("publication")
    comments = get_table("publication_comment")
    query = select(publications.c.publication_comment_id).where(publications.c.id == int_or_none(publication_id))
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

    values["date_modified"] = datetime.now()

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
                "msg": "Created comment {} for publication {} with values {}".format(comment_id, publication_id, str(values)),
                "comment_id": comment_id
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

    publications = get_table("publication")
    manuscripts = get_table("publication_manuscript")
    query = select(publications).where(publications.c.id == int_or_none(publication_id))
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

    manuscripts = get_table("publication_manuscript")
    query = select(manuscripts).where(manuscripts.c.id == int_or_none(manuscript_id))
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

    values["date_modified"] = datetime.now()

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

    publications = get_table("publication")
    versions = get_table("publication_version")
    query = select(publications).where(publications.c.id == int_or_none(publication_id))
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

    versions = get_table("publication_version")
    query = select(versions).where(versions.c.id == int_or_none(version_id))
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

    values["date_modified"] = datetime.now()

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
    external_url = request_data.get("external_url", None)
    collections = get_table("publication_facsimile_collection")
    query = select(collections).where(collections.c.id == int_or_none(collection_id))
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
    if external_url is not None:
        values["external_url"] = external_url
    values["date_modified"] = datetime.now()

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
    collections = get_table("publication_collection")
    intros = get_table("publication_collection_introduction")
    titles = get_table("publication_collection_title")

    query = select(collections).where(collections.c.id == int_or_none(collection_id))
    connection = db_engine.connect()
    collection_result = connection.execute(query).fetchone()
    if collection_result is None:
        connection.close()
        return jsonify("No such publication collection exists"), 404

    intro_id = int_or_none(collection_result["publication_collection_introduction_id"])
    title_id = int_or_none(collection_result["publication_collection_title_id"])
    intro_query = select(intros.c.published, intros.c.original_filename).where(intros.c.id == intro_id)
    title_query = select(titles.c.published, titles.c.original_filename).where(titles.c.id == title_id)

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
