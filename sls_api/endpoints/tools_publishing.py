import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import db_engine, metadata, project_permission_required


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
    query = select([projects.c.id]).where(projects.c.id == int(project_id))
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


@publishing_tools.route("/projects/<project_id>/", methods=["DELETE"])
@jwt_required
def delete_project(project_id):
    # TODO project delete (logical?)
    pass


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
    query = select([collections.c.id]).where(collections.c.id == int(collection_id))
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


@publishing_tools.route("/<project>/publication_collection/<collection_id>/", methods=["DELETE"])
@project_permission_required
def delete_publication_collection(project, collection_id):
    # TODO publication collection delete (logical?)
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/")
@project_permission_required
def get_intro(project, collection_id):
    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    introductions = Table("publication_collection_introduction", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_introduction_id]).where(collections.c.id == int(collection_id))
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
    query = select([collections.c.publication_collection_introduction_id]).where(collections.c.id == int(collection_id))
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


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/", methods=["DELETE"])
@project_permission_required
def delete_intro(project, collection_id):
    # TODO collection introduction delete (logical?)
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/")
@project_permission_required
def get_title(project, collection_id):
    collections = Table("publication_collection", metadata, autoload=True, autoload_with=db_engine)
    titles = Table("publication_collection_title", metadata, autoload=True, autoload_with=db_engine)
    query = select([collections.c.publication_collection_title_id]).where(collections.c.id == int(collection_id))
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
    query = select([collections.c.publication_collection_title_id]).where(collections.c.id == int(collection_id))
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


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/", methods=["DELETE"])
@project_permission_required
def delete_title(project, collection_id):
    # TODO collection title delete (logical?)
    pass


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
    query = select([publications.c.id]).where(publications.c.id == int(publication_id))
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


@publishing_tools.route("/<project>/publication/<publication_id>/", methods=["DELETE"])
@project_permission_required
def delete_publication(project, publication_id):
    # TODO publication delete (logical?)
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/comment/edit/", methods=["POST"])
@project_permission_required
def edit_comment(project, publication_id):
    """
    Takes "filename" and/or "published" as JSON data
    Returns "msg" and "comment_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    comments = Table("publication_comment", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications.c.publication_comment_id]).where(publications.c.id == int(publication_id))
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
        update = comments.update().where(comments.c.id == int(comment_id)).values(**values)
        connection.execute(update)
        connection.close()
        return jsonify({
            "msg": "Updated comment {} with values {}".format(comment_id, str(values)),
            "comment_id": comment_id
        })
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@publishing_tools.route("/<project>/publication/<publication_id>/comment/", methods=["DELETE"])
@project_permission_required
def delete_comment(project, publication_id):
    # TODO comment delete (logical?)
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/manuscripts/new/", methods=["POST"])
@project_permission_required
def add_manuscript(project, publication_id):
    """
    Takes "title", "filename", "published" as JSON data
    Returns "msg" and "manuscript_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    manuscripts = Table("publication_manuscript", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications]).where(publications.c.id == int(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

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
    Takes "title", "filename", "published" as JSON data
    Returns "msg" and "manuscript_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    manuscripts = Table("publication_manuscript", metadata, autoload=True, autoload_with=db_engine)
    query = select([manuscripts]).where(manuscripts.c.id == int(manuscript_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

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


@publishing_tools.route("/<project>/manuscripts/<manuscript_id>/", methods=["DELETE"])
@project_permission_required
def delete_manuscript(project, manuscript_id):
    # TODO manuscript delete (logical?)
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/versions/new/", methods=["POST"])
@project_permission_required
def add_version(project, publication_id):
    """
    Takes "title", "filename", "published" as JSON data
    Returns "msg" and "version_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
    versions = Table("publication_version", metadata, autoload=True, autoload_with=db_engine)
    query = select([publications]).where(publications.c.id == int(publication_id))
    connection = db_engine.connect()

    result = connection.execute(query).fetchone()
    if result is None:
        connection.close()
        return jsonify("No such publication exists."), 404

    values = {}
    if title is not None:
        values["name"] = title
    if filename is not None:
        values["original_filename"] = filename
    if published is not None:
        values["published"] = published

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
    Takes "title", "filename", "published" as JSON data
    Returns "msg" and "manuscript_id" on success, otherwise 40x
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    title = request_data.get("title", None)
    filename = request_data.get("filename", None)
    published = request_data.get("published", None)

    versions = Table("publication_version", metadata, autoload=True, autoload_with=db_engine)
    query = select([versions]).where(versions.c.id == int(version_id))
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


@publishing_tools.route("/<project>/versions/<version_id>/", methods=["DELETE"])
@project_permission_required
def delete_version(project, version_id):
    # TODO version delete (logical?)
    pass


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
    query = select([collections]).where(collections.c.id == int(collection_id))
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


@publishing_tools.route("/<project>/facsimile_collection/<collection_id>/", methods=["DELETE"])
@project_permission_required
def delete_facsimile_collection(project, collection_id):
    # TODO facsimile collection delete (logical?)
    pass
