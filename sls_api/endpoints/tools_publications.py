from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, metadata, project_permission_required


publication_tools = Blueprint("publication_tools", __name__)


@publication_tools.route("/<project>/publications")
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


@publication_tools.route("/<project>/publication/<publication_id>")
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
    connection.close()
    return jsonify(result)


@publication_tools.route("/<project>/publication/<publication_id>/versions")
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


@publication_tools.route("/<project>/publication/<publication_id>/manuscripts")
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


@publication_tools.route("/<project>/publication/<publication_id>/fascimiles")
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


@publication_tools.route("/<project>/publication/<publication_id>/comments")
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


@publication_tools.route("/<project>/publication/<publication_id>/link_file", methods=["POST"])
@project_permission_required
def link_file_to_publication(project, publication_id):
    """
    Link an XML file to a publication,
    creating the appropriate publicationComment, publicationManuscript, or publicationVersion object.

    POST data MUST be in JSON format

    POST data MUST contain the following:
    file_type: one of [comment, manuscript, version] indicating which type of file the given file_path points to
    file_path: path to the file to be linked

    POST data SHOULD also contain the following:
    datePublishedExternally: date of external publication
    published: 0 or 1, is this file published and ready for viewing
    publishedBy: person responsible for publishing

    POST data MAY also contain:
    legacyId: legacy ID for this publication file object
    type: Type of file link, for Manuscripts and Versions
    sectionId: Publication section or chapter number, for Manuscripts and Versions
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "file_path" not in request_data or "file_type" not in request_data or request_data.get("file_type", None) not in ["comment", "manuscript", "version"]:
        return jsonify({"msg": "POST data JSON doesn't contain required data."}), 400

    file_type = request_data["file_type"]

    connection = db_engine.connect()

    if file_type == "comment":
        comments = Table("publicationComment", metadata, autoload=True, autoload_with=db_engine)
        publications = Table("publication", metadata, autoload=True, autoload_with=db_engine)
        transaction = connection.begin()
        new_comment = {
            "originalFileName": request_data.get("file_path"),
            "datePublishedExternally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "publishedBy": request_data.get("publishedBy", None),
            "legacyId": request_data.get("legacyId", None)
        }
        ins = comments.insert()
        try:
            result = connection.execute(ins, **new_comment)
            new_row = select([comments]).where(comments.c.id == result.inserted_primary_key[0])
            new_row = dict(connection.execute(new_row).fetchone())

            # update publication object in database with new publicationComment ID
            update_stmt = publications.update().where(publications.c.id == int(publication_id)). \
                values(publications.c.publicationComment_id == result.inserted_primary_key[0])
            connection.execute(update_stmt)

            transaction.commit()
            result = {
                "msg": "Created new publicationComment with ID {}".format(result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
        except Exception as e:
            transaction.rollback()
            result = {
                "msg": "Failed to create new publicationComment object",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        new_object = {
            "originalFileName": request_data.get("file_path"),
            "publication_id": int(publication_id),
            "datePublishedExternally": request_data.get("datePublishedExternally", None),
            "published": request_data.get("published", None),
            "publishedBy": request_data.get("publishedBy", None),
            "legacyId": request_data.get("legacyId", None),
            "type": request_data.get("type", None),
            "sectionId": request_data.get("sectionId", None)
        }
        if file_type == "manuscript":
            table = Table("publicationManuscript", metadata, autoload=True, autoload_with=False)
        else:
            table = Table("publicationVersion", metadata, autoload=True, autoload_with=False)
        ins = table.insert()
        try:
            result = connection.execute(ins, **new_object)
            new_row = select([table]).where(table.c.id == result.inserted_primary_key[0])
            new_row = dict(connection.execute(new_row).fetchone())
            result = {
                "msg": "Created new publication{} with ID {}".format(file_type.capitalize(), result.inserted_primary_key[0]),
                "row": new_row
            }
            return jsonify(result), 201
        except Exception as e:
            result = {
                "msg": "Failed to create new object",
                "reason": str(e)
            }
            return jsonify(result), 500

        finally:
            connection.close()
