from flask import Blueprint, jsonify, request
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, metadata, \
    project_permission_required, select_all_from_table


collection_tools = Blueprint("collection_tools", __name__)


@collection_tools.route("/<project>/fascimile_collection/new", methods=["POST"])
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
    pageComment: Commentary on page numbering
    externalURL: Externally viewable URL for this fascimile collection
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


@collection_tools.route("/<project>/fascimile_collection/list")
@project_permission_required
def list_fascimile_collections(project):
    """
    List all available publicationFascimileCollections
    """
    return select_all_from_table("publicationFascimileCollections")


@collection_tools.route("/<project>/fascimile_collection/<collection_id>/link", methods=["POST"])
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
    sectionId: Section or chapter number for this particular fascimile
    pageNr: Page number for link
    priority: Priority number for this link
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


@collection_tools.route("/<project>/fascimile_collection/<collection_id>/list_links")
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


@collection_tools.route("/<project>/publication_collection/list")
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


@collection_tools.route("/<project>/publication_collection/new", methods=["POST"])
@project_permission_required
def new_publication_collection(project):
    """
    Create a new publicationCollection object and associated Introduction and Title objects.

    POST data MUST be in JSON format

    POST data SHOULD contain the following:
    name: publication collection name or title
    datePublishedExternally: date of external publishing for collection
    published: 0 or 1, is collection published or not

    POST data MAY also contain the following
    intro_legacyID: legacy ID for publicationCollectionIntroduction
    title_legacyID: legacy ID for publicationCollectionTitle
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    collections = Table("publicationCollection", metadata, autoload=True, autoload_with=db_engine)
    introductions = Table("publicationCollectionIntroduction", metadata, autoload=True, autoload_with=db_engine)
    titles = Table("publicationCollectionTitle", metadata, autoload=True, autoload_with=db_engine)

    connection = db_engine.connect()
    transaction = connection.begin()
    try:
        new_intro = {
            "datePublishedExternally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "legacyId": request_data.get("intro_legacyID", None)
        }

        new_title = {
            "datePublishedExternally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "legacyId": request_data.get("title_legacyID", None)
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
            "datePublishedExternally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "publicationCollectionIntroduction_id": new_intro_row["id"],
            "publicationCollectionTitle_id": new_title_row["id"]
        }

        ins = collections.insert()
        result = connection.execute(ins, **new_collection)
        new_collection_row = select([collections]).where(collections.c.id == result.inserted_primary_key[0])
        new_collection_row = dict(connection.execute(new_collection_row).fetchone())
        transaction.commit()

        return jsonify({
            "msg": "New publicationCollection created.",
            "new_collection": new_collection_row,
            "new_collection_intro": new_intro_row,
            "new_collection_title": new_title_row
        }), 201
    except Exception as e:
        transaction.rollback()
        result = {
            "msg": "Failed to create new publicationCollection object",
            "reason": str(e)
        }
        return jsonify(result), 500
    finally:
        connection.close()


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications")
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
                "msg": "Found collection not part of project {!r} with ID {}.".format(project, project_id)
            }
        ), 400
    statement = select([publications]).where(publications.c.publicationCollection_id == int(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        result.append(dict(row))
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/publication_collection/<collection_id>/publications/new", methods=["POST"])
@project_permission_required
def new_publication(project, collection_id):
    """
    Create a new publication object as part of the given publicationCollection

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
        "publicationComment_id": request_data.get("publicationComment_id", None),
        "datePublishedExternally": request_data.get("datePublishedExternally", None),
        "published": request_data.get("published", None),
        "legacyId": request_data.get("legacyId", None),
        "publishedBy": request_data.get("publishedBy", None),
        "originalFilename": request_data.get("originalFileName", None),
        "genre": request_data.get("genre", None),
        "publicationGroup_id": request_data.get("publicationGroup_id", None),
        "originalPublicationDate": request_data.get("originalPublicationDate", None)
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
