from flask import Blueprint, jsonify, request
from sqlalchemy import select, text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, select_all_from_table


collection_tools = Blueprint("collection_tools", __name__)


@collection_tools.route("/<project>/facsimile_collection/new/", methods=["POST"])
@project_permission_required
def create_facsimile_collection(project):
    """
    Create a new publication_facsimile_collection

    POST data MUST be in JSON format.

    POST data SHOULD contain:
    title: collection type
    description: collection description
    folderPath: path to facsimiles for this collection

    POST data MAY also contain:
    numberOfPages: total number of pages in this collection
    startPageNumber: number for starting page of this collection
    pageComment: Commentary on page numbering
    externalURL: Externally viewable URL for this facsimile collection
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    collections = get_table("publication_facsimile_collection")
    connection = db_engine.connect()
    insert = collections.insert()

    new_collection = {
        "title": request_data.get("title", None),
        "description": request_data.get("description", None),
        "folder_path": request_data.get("folderPath", None),
        "number_of_pages": request_data.get("numberOfPages", None),
        "start_page_number": request_data.get("startPageNumber", None)
    }
    try:
        result = connection.execute(insert, **new_collection)
        new_row = select([collections]).where(collections.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new publication_facsimile_collection with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publication_facsimile_collection",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@collection_tools.route("/<project>/facsimile_collection/list/")
@project_permission_required
def list_facsimile_collections(project):
    """
    List all publication_collection objects for a given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    statement = """ select * from publication_facsimile_collection where deleted != 1 AND (
                    id in
                    (
                        select publication_facsimile_collection_id from publication_facsimile where publication_id in (
                            select id from publication where publication_collection_id in (
                                select id from publication_collection where project_id = :project_id and deleted != 1
                            )
                        )
                    ) or
                    id not in
                    (
                        select publication_facsimile_collection_id from publication_facsimile where publication_id in (
                            select id from publication where publication_collection_id in (
                                select id from publication_collection where deleted != 1
                            )
                        )
                    )
                )"""
    statement = text(statement).bindparams(project_id=project_id)
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/link/", methods=["POST"])
@project_permission_required
def link_facsimile_collection_to_publication(project, collection_id):
    """
    Link a publication_facsimile_collection to a publication through publication_facsimile table

    POST data MUST be in JSON format.

    POST data MUST contain the following:
    publication_id: ID for the publication to link to

    POST data MAY also contain the following:
    publicationManuscript_id: ID for the specific publication manuscript to link to
    publicationVersion_id: ID for the specific publication version to link to
    sectionId: Section or chapter number for this particular facsimile
    pageNr: Page number for link
    priority: Priority number for this link
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "publication_id" not in request_data:
        return jsonify({"msg": "No publication_id in POST data."}), 400

    connection = db_engine.connect()
    publication_id = int_or_none(request_data["publication_id"])
    project_id = get_project_id_from_name(project)

    publication_facsimiles = get_table("publication_facsimile")
    publication_collections = get_table("publication_collection")
    publications = get_table("publication")

    statement = select([publications.c.publication_collection_id]).where(publications.c.id == publication_id)
    result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404
    publication_collection_id = int_or_none(result[0]["publication_collection_id"])

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

    insert = publication_facsimiles.insert()
    new_facsimile = {
        "publication_facsimile_collection_id": collection_id,
        "publication_id": publication_id,
        "publication_manuscript_id": request_data.get("publication_manuscript_id", None),
        "publication_version_id": request_data.get("publication_version_id", None)
    }
    try:
        result = connection.execute(insert, **new_facsimile)
        new_row = select([publication_facsimiles]).where(publication_facsimiles.c.id == result.inserted_primary_key[0])
        new_row = dict(connection.execute(new_row).fetchone())
        result = {
            "msg": "Created new publication_facsimile with ID {}".format(result.inserted_primary_key[0]),
            "row": new_row
        }
        return jsonify(result), 201
    except Exception as e:
        result = {
            "msg": "Failed to create new publication_facsimile",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/")
@project_permission_required
def list_facsimile_collection_links(project, collection_id):
    """
    List all publication_facsimile objects in the given publication_facsimile_collection
    """
    connection = db_engine.connect()
    facsimiles = get_table("publication_facsimile")
    statement = select([facsimiles]).where(facsimiles.c.publication_facsimile_collection_id == int_or_none(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/publication_collection/list/")
@project_permission_required
def list_publication_collections(project):
    """
    List all publication_collection objects for a given project
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    collections = get_table("publication_collection")
    statement = select([collections]).where(collections.c.project_id == int_or_none(project_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/publication_collection/new/", methods=["POST"])
@project_permission_required
def new_publication_collection(project):
    """
    Create a new publication_collection object and associated Introduction and Title objects.

    POST data MUST be in JSON format

    POST data SHOULD contain the following:
    name: publication collection name or title
    datePublishedExternally: date of external publishing for collection
    published: 0 or 1, is collection published or not

    POST data MAY also contain the following
    intro_legacyID: legacy ID for publication_collection_introduction
    title_legacyID: legacy ID for publication_collection_title
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    collections = get_table("publication_collection")
    introductions = get_table("publication_collection_introduction")
    titles = get_table("publication_collection_title")

    connection = db_engine.connect()
    transaction = connection.begin()
    try:
        new_intro = {
            "date_published_externally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "legacy_id": request_data.get("intro_legacyID", None)
        }

        new_title = {
            "date_published_externally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "legacy_id": request_data.get("title_legacyID", None)
        }

        ins = introductions.insert()
        result = connection.execute(ins, **new_intro)
        new_intro_row = select([introductions]).where(introductions.c.id == result.inserted_primary_key[0])
        new_intro_row = dict(connection.execute(new_intro_row).fetchone())

        ins = titles.insert()
        result = connection.execute(ins, **new_title)
        new_title_row = select([titles]).where(titles.c.id == result.inserted_primary_key[0])
        new_title_row = dict(connection.execute(new_title_row).fetchone())

        new_collection = {
            "project_id": get_project_id_from_name(project),
            "name": request_data.get("name", None),
            "date_published_externally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "publication_collection_introduction_id": new_intro_row["id"],
            "publication_collection_title_id": new_title_row["id"]
        }

        ins = collections.insert()
        result = connection.execute(ins, **new_collection)
        new_collection_row = select([collections]).where(collections.c.id == result.inserted_primary_key[0])
        new_collection_row = dict(connection.execute(new_collection_row).fetchone())
        transaction.commit()

        return jsonify({
            "msg": "New publication_collection created.",
            "new_collection": new_collection_row,
            "new_collection_intro": new_intro_row,
            "new_collection_title": new_title_row
        }), 201
    except Exception as e:
        transaction.rollback()
        result = {
            "msg": "Failed to create new publication_collection object",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/")
@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/<order_by>/")
@project_permission_required
def list_publications(project, collection_id, order_by="id"):
    """
    List all publications in a given collection
    """
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    collections = get_table("publication_collection")
    publications = get_table("publication")
    statement = select([collections]).where(collections.c.id == int_or_none(collection_id)).order_by(str(order_by))
    rows = connection.execute(statement).fetchall()
    if len(rows) != 1:
        return jsonify(
            {
                "msg": "Could not find collection in database."
            }
        ), 404
    elif rows[0]["project_id"] != int_or_none(project_id):
        return jsonify(
            {
                "msg": "Found collection not part of project {!r} with ID {}.".format(project, project_id)
            }
        ), 400
    statement = select([publications]).where(publications.c.publication_collection_id == int_or_none(collection_id)).order_by(str(order_by))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/new/", methods=["POST"])
@project_permission_required
def new_publication(project, collection_id):
    """
    Create a new publication object as part of the given publication_collection

    POST data MUST be in JSON format.

    POST data SHOULD contain the following:
    name: publication name

    POST data MAY also contain the following:
    publicationComment_id: ID for related publicationComment object
    datePublishedExternally: date of external publication for publication
    published: publish status for publication
    legacyId: legacy ID for publication
    publishedBy: person responsible for publishing the publication
    originalFilename: filepath to publication XML file
    genre: Genre for this publication
    publicationGroup_id: ID for related publicationGroup, used to group publications for easy publishing of large numbers of publications
    originalPublicationDate: Date of original publication for physical equivalent
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    project_id = get_project_id_from_name(project)

    connection = db_engine.connect()
    collections = get_table("publication_collection")
    publications = get_table("publication")

    statement = select([collections.c.project_id]).where(collections.c.id == int_or_none(collection_id))
    result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "publication_collection not found."
            }
        ), 404

    if result[0]["project_id"] != project_id:
        return jsonify(
            {
                "msg": "publication_collection {} does not belong to project {!r}".format(collection_id, project)
            }
        ), 400

    insert = publications.insert()

    publication = {
        "name": request_data.get("name", None),
        "publication_comment_id": request_data.get("publicationComment_id", None),
        "date_published_externally": request_data.get("datePublishedExternally", None),
        "published": request_data.get("published", None),
        "legacy_id": request_data.get("legacyId", None),
        "published_by": request_data.get("publishedBy", None),
        "original_filename": request_data.get("originalFileName", None),
        "genre": request_data.get("genre", None),
        "publication_group_id": request_data.get("publicationGroup_id", None),
        "original_publication_date": request_data.get("originalPublicationDate", None),
        "publication_collection_id": int_or_none(collection_id)
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
