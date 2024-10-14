from flask import Blueprint, jsonify, request
from sqlalchemy import select, text
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, \
    int_or_none, project_permission_required, validate_int


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

    new_collection = {
        "title": request_data.get("title", None),
        "description": request_data.get("description", None),
        "folder_path": request_data.get("folderPath", None),
        "external_url": request_data.get("externalUrl", None),
        "number_of_pages": request_data.get("numberOfPages", None),
        "start_page_number": request_data.get("startPageNumber", None)
    }
    insert = collections.insert().values(**new_collection)
    try:
        with connection.begin():
            result = connection.execute(insert)
            new_row = select(collections).where(collections.c.id == result.inserted_primary_key[0])

            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            else:
                new_row = {}
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


@collection_tools.route("/<project>/facsimile_collection/<facsimile_collection_id>/edit/", methods=["POST"])
@project_permission_required
def edit_facsimile_collection(project, facsimile_collection_id):
    """
    Edit a facsimile_collection object in the database

    POST data MUST be in JSON format.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    facsimile_collections = get_table("publication_facsimile_collection")

    connection = db_engine.connect()
    with connection.begin():
        facsimile_collections_query = select(facsimile_collections.c.id).where(facsimile_collections.c.id == int_or_none(facsimile_collection_id))
        facsimile_collections_row = connection.execute(facsimile_collections_query).fetchone()
    if facsimile_collections_row is None:
        return jsonify({"msg": "No facsimile collection with an ID of {} exists.".format(facsimile_collection_id)}), 404

    title = request_data.get("title", None)
    number_of_pages = request_data.get("number_of_pages", 0)
    start_page_number = request_data.get("start_page_number", 0)
    description = request_data.get("description", None)
    folder_path = request_data.get("folder_path", None)
    page_comment = request_data.get("page_comment", None)
    external_url = request_data.get("external_url", None)

    values = {}
    if title is not None:
        values["title"] = title
    if number_of_pages is not None:
        values["number_of_pages"] = number_of_pages
    if start_page_number is not None:
        values["start_page_number"] = start_page_number
    if description is not None:
        values["description"] = description
    if folder_path is not None:
        values["folder_path"] = folder_path
    if page_comment is not None:
        values["page_comment"] = page_comment
    if external_url is not None:
        values["external_url"] = external_url

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = facsimile_collections.update().where(facsimile_collections.c.id == int(facsimile_collection_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated facsimile_collection {} with values {}".format(int(facsimile_collection_id), str(values)),
                    "facsimile_collection_id": int(facsimile_collection_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update facsimile_collections.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


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
        if row is not None:
            result.append(row._asdict())
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
    with connection.begin():
        statement = select(publications.c.publication_collection_id).where(publications.c.id == publication_id)
        result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404
    publication_collection_id = int_or_none(result[0].publication_collection_id)

    with connection.begin():
        statement = select(publication_collections.c.project_id).where(publication_collections.c.id == publication_collection_id)
        result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "Could not find publication collection for publication, unable to verify that publication belongs to {!r}".format(project)
            }
        ), 404

    if result[0].project_id != project_id:
        return jsonify(
            {
                "msg": "Publication {} appears to not belong to project {!r}".format(publication_id, project)
            }
        ), 400

    new_facsimile = {
        "publication_facsimile_collection_id": collection_id,
        "publication_id": publication_id,
        "publication_manuscript_id": request_data.get("publication_manuscript_id", None),
        "publication_version_id": request_data.get("publication_version_id", None),
        "page_nr": request_data.get("page", 0),
        "section_id": request_data.get("section_id", 0),
        "priority": request_data.get("priority", 0),
        "type": request_data.get("type", 0)
    }
    try:
        with connection.begin():
            insert = publication_facsimiles.insert().values(**new_facsimile)
            result = connection.execute(insert)
            new_row = select(publication_facsimiles).where(publication_facsimiles.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            else:
                new_row = {}
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


@collection_tools.route("/<project>/facsimile_collection/facsimile/edit/", methods=["POST"])
@project_permission_required
def edit_facsimile(project):
    """
    Edit a facsimile object in the database

    POST data MUST be in JSON format.

    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    facsimile_id = request_data.get("id", None)
    facsimile = get_table("publication_facsimile")

    connection = db_engine.connect()
    with connection.begin():
        facsimile_query = select(facsimile.c.id).where(facsimile.c.id == int_or_none(facsimile_id))
        facsimile_row = connection.execute(facsimile_query).fetchone()
    if facsimile_row is None:
        return jsonify({"msg": "No facsimile with an ID of {} exists.".format(facsimile_id)}), 404

    # facsimile_collection_id = request_data.get("facsimile_collection_id", None)
    page = request_data.get("page", None)
    priority = request_data.get("priority", None)
    type = request_data.get("type", None)

    values = {}
    if page is not None:
        values["page_nr"] = page
    if type is not None:
        values["type"] = type
    if priority is not None:
        values["priority"] = priority

    values["date_modified"] = datetime.now()

    if len(values) > 0:
        try:
            with connection.begin():
                update = facsimile.update().where(facsimile.c.id == int(facsimile_id)).values(**values)
                connection.execute(update)
                return jsonify({
                    "msg": "Updated facsimile {} with values {}".format(int(facsimile_id), str(values)),
                    "facsimile_id": int(facsimile_id)
                })
        except Exception as e:
            result = {
                "msg": "Failed to update facsimile.",
                "reason": str(e)
            }
            return jsonify(result), 500
        finally:
            connection.close()
    else:
        connection.close()
        return jsonify("No valid update values given."), 400


@collection_tools.route("/<project>/facsimile_collection/<collection_id>/list_links/")
@project_permission_required
def list_facsimile_collection_links(project, collection_id):
    """
    List all publication_facsimile objects in the given publication_facsimile_collection
    """
    connection = db_engine.connect()
    facsimiles = get_table("publication_facsimile")
    statement = select(facsimiles).where(facsimiles.c.publication_facsimile_collection_id == int_or_none(collection_id))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
    connection.close()
    return jsonify(result)


@collection_tools.route("/<project>/facsimile_publication/delete/<f_pub_id>", methods=["DELETE"])
@project_permission_required
def delete_facsimile_collection_link(project, f_pub_id):
    """
    List all publication_facsimile objects in the given publication_facsimile_collection
    """
    connection = db_engine.connect()
    publication_facsimile = get_table("publication_facsimile")
    values = {
        'deleted': 1,
        "date_modified": datetime.now()
    }
    with connection.begin():
        update = publication_facsimile.update().where(publication_facsimile.c.id == int(f_pub_id)).values(**values)
        connection.execute(update)
    connection.close()
    result = {
        "msg": "Deleted publication_facsimile"
    }
    return jsonify(result), 200


@collection_tools.route("/<project>/publication_collection/list/")
@project_permission_required
def list_publication_collections(project):
    """
    List all publication collections for a given project.

    URL Path Parameter:
    - project (str, required): The name of the project to retrieve publication collections for.

    Returns:
        JSON: A list of all publication collection objects associated with the given project, or an error message.

    Example Request:
        GET /<project>/publication_collection/list/

    Example Response (Success):
        [
            {
                "id": 1,
                "name": "Collection Title",
                "published": 1,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "date_published_externally": null,
                "deleted": 0,
                "legacy_id": null,
                "project_id": 101,
                "publication_collection_title_id": 55,
                "publication_collection_introduction_id": 75,
                "name_translation_id": null,
                "collection_title_filename": "title_file.xml",
                "collection_intro_filename": "intro_file.xml",
                "collection_title_published": 1,
                "collection_intro_published": 1
            }
        ]

    Example Response (Error):
        {
            "msg": "No such project exists."
        }

    Status Codes:
        200 - OK: The request was successful, and the publication collections are returned.
        400 - Bad Request: The project does not exist.
        500 - Internal Server Error: Failed to retrieve the publication collections.
    """
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "No such project exists."}), 400

    statement = """
        SELECT
            pc.id,
            pc.name,
            pc.published,
            pc.date_created,
            pc.date_modified,
            pc.date_published_externally,
            pc.deleted,
            pc.legacy_id,
            pc.project_id,
            pc.publication_collection_title_id,
            pc.publication_collection_introduction_id,
            pc.name_translation_id,
            pct.original_filename AS collection_title_filename,
            pci.original_filename AS collection_intro_filename,
            pct.published AS collection_title_published,
            pci.published AS collection_intro_published
        FROM
            publication_collection pc
        LEFT JOIN
            publication_collection_title pct
            ON pct.id = pc.publication_collection_title_id
        LEFT JOIN
            publication_collection_introduction pci
            ON pci.id = pc.publication_collection_introduction_id
        WHERE
            pc.project_id = :project_id
        ORDER BY
            pc.id
    """

    try:
        with db_engine.connect() as connection:
            rows = connection.execute(
                text(statement),
                {"project_id": project_id}
            ).fetchall()
            result = [row._asdict() for row in rows]
            return jsonify(result)
    except Exception as e:
        return jsonify({"msg": "Failed to retrieve project publication collections.",
                        "reason": str(e)}), 500


@collection_tools.route("/<project>/publication_collection/new/", methods=["POST"])
@project_permission_required
def new_publication_collection(project):
    """
    Create a new publication collection in the specified project.

    Parameters:
    - project (str): The name of the project.

    POST data parameters in JSON format:
    - published (int, required): The publication status of the collection. Must be an
      integer with values 0, 1 or 2. Recommended initial value is 1.
    - name (str): The name/title of the publication collection.

    Returns:
        JSON: A success message with the id of the inserted publication collection,
        or an error message.

    Example Request:
        POST /projectname/publication_collection/new/
        Body:
        {
            "name": "My New Collection",
            "published": 1
        }

    Example Response (Success):
        {
            "msg": "Publication collection inserted successfully.",
            "id": 456
        }

    Example Response (Error):
        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:
        201 - Created: The publication collection was inserted successfully.
        400 - Bad Request: Invalid project name, invalid field values, or no valid
              fields provided for insertion.
        500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project is valid
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Verify that the required 'published' field was provided
    if "published" not in request_data:
        return jsonify({"msg": "'published' field is required."}), 400

    # Start building values dictionary for the insert
    values = {}

    # List of fields to check in request_data
    fields = ["name", "published"]

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other fields are strings
            if field == "published":
                if not validate_int(request_data["published"], 0, 2):
                    return jsonify({"msg": "Field 'published' must be an integer with value 0, 1 or 2."}), 400
            else:
                # Convert remaining fields to string
                request_data[field] = str(request_data[field])

            # Add the field to the field_names list for the query construction
            values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided for insertion."}), 400

    # Add project_id to insert values
    values["project_id"] = project_id

    publication_collection = get_table("publication_collection")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Execute the insert statement
                statement = (
                    publication_collection.insert()
                    .values(**values)
                    .returning(publication_collection.c.id)
                )
                result = connection.execute(statement)
                pc_id = result.fetchone()[0]

                return jsonify({"msg": "Publication collection inserted successfully.",
                                "id": pc_id}), 201

    except Exception as e:
        return jsonify({"msg": "Failed to insert new publication collection.",
                        "reason": str(e)}), 500


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
    statement = select(collections).where(collections.c.id == int_or_none(collection_id)).order_by(str(order_by))
    rows = connection.execute(statement).fetchall()
    if len(rows) != 1:
        return jsonify(
            {
                "msg": "Could not find collection in database."
            }
        ), 404
    elif rows[0].project_id != int_or_none(project_id):
        return jsonify(
            {
                "msg": "Found collection not part of project {!r} with ID {}.".format(project, project_id)
            }
        ), 400
    statement = select(publications).where(publications.c.publication_collection_id == int_or_none(collection_id)).order_by(str(order_by))
    rows = connection.execute(statement).fetchall()
    result = []
    for row in rows:
        if row is not None:
            result.append(row._asdict())
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
    language: language code (ISO 639-1) of the primary language of the publication text
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    project_id = get_project_id_from_name(project)

    connection = db_engine.connect()
    collections = get_table("publication_collection")
    publications = get_table("publication")

    with connection.begin():
        statement = select(collections.c.project_id).where(collections.c.id == int_or_none(collection_id))
        result = connection.execute(statement).fetchall()
    if len(result) != 1:
        return jsonify(
            {
                "msg": "publication_collection not found."
            }
        ), 404

    if result[0].project_id != project_id:
        return jsonify(
            {
                "msg": "publication_collection {} does not belong to project {!r}".format(collection_id, project)
            }
        ), 400

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
        "publication_collection_id": int_or_none(collection_id),
        "language": request_data.get("language", None)
    }
    try:
        with connection.begin():
            insert = publications.insert().values(**publication)
            result = connection.execute(insert)
            new_row = select(publications).where(publications.c.id == result.inserted_primary_key[0])
            new_row = connection.execute(new_row).fetchone()
            if new_row is not None:
                new_row = new_row._asdict()
            else:
                new_row = {}
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
