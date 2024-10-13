import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import select, text
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required


publishing_tools = Blueprint("publishing_tools", __name__)

logger = logging.getLogger("sls_api.tools.publishing")


@publishing_tools.route("/projects/new/", methods=["POST"])
@jwt_required()
def add_new_project():
    """
    Create a new project.

    POST data parameters in JSON format:
    - name (str, required): The name/title of the new project.

    Returns:
        JSON: A success message with the id of the inserted project (`project_id`), or an error message.

    Example Request:
        POST /projects/new/
        Body:
        {
            "name": "My New Project"
        }

    Example Response (Success):
        {
            "msg": "Created new project.",
            "project_id": 123
        }

    Example Response (Error):
        {
            "msg": "No data provided."
        }

    Status Codes:
        201 - Created: The project was inserted successfully.
        400 - Bad Request: No data was provided in the request, or required fields are missing.
        500 - Internal Server Error: Database query or execution failed.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400
    if "name" not in request_data:
        return jsonify({"msg": "Project name required."}), 400
    name = request_data.get("name", None)

    projects = get_table("project")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                ins = projects.insert().values(name=name)
                result = connection.execute(ins)

                return jsonify({
                    "msg": "Created new project.",
                    "project_id": int(result.inserted_primary_key[0])
                }), 201
    except Exception as e:
        return jsonify({"msg": "Failed to create new project.",
                        "reason": str(e)}), 500


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
    connection = db_engine.connect()
    with connection.begin():
        query = select(projects.c.id).where(projects.c.id == int_or_none(project_id))
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
        with connection.begin():
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
    Edit a publication collection in the specified project by updating its fields.

    Parameters:
    - project (str): The name of the project.
    - collection_id (str): The id of the publication collection to be updated. Must be a valid positive integer.

    Optional POST data parameters in JSON format (at least one required):
    - name (str or null): The name/title of the publication collection.
    - published (int): The publication status. Must be an integer with values 0, 1 or 2.
    - deleted (int): Soft delete flag. Must be an integer with values 0 or 1.

    Returns:
        JSON: A success message with the updated publication collection id, or an error message.

    Example Request:
        POST /projectname/publication_collection/456/edit/
        Body:
        {
            "name": "Updated Collection Name",
            "published": 1
        }

    Example Response (Success):
        {
            "msg": "Publication collection with id 456 updated successfully."
        }

    Example Response (Error):
        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:
        200 - OK: The publication collection was updated successfully.
        400 - Bad Request: Invalid collection_id, invalid field values, or no valid fields provided for the update.
        404 - Not Found: No publication collection with the given collection_id exists or no changes were made.
        500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project is valid
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Verify that collection_id is an integer
    collection_id = int_or_none(collection_id)
    if not collection_id or collection_id < 1:
        return jsonify({"msg": "Invalid collection_id."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # List of fields to check in request_data
    fields = ["name", "published", "deleted"]

    # Start building the update query
    query = "UPDATE publication_collection SET "
    values = {}
    set_clauses = []

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Allow setting the 'name' field to NULL
            if request_data[field] is None and field == "name":
                set_clauses.append(f"{field} = NULL")
            else:
                # Validate integer field values and ensure all other fields are strings
                if field == "published":
                    if not isinstance(request_data[field], int) or request_data[field] < 0 or request_data[field] > 2:
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
                elif field == "deleted":
                    if not isinstance(request_data[field], int) or request_data[field] < 0 or request_data[field] > 1:
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the set_clauses and values for the query
                set_clauses.append(f"{field} = :{field}")
                values[field] = request_data[field]

    if not set_clauses:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified field to SET clauses
    set_clauses.append("date_modified = :date_modified")
    values["date_modified"] = datetime.now()

    # Join all SET clauses with commas
    query += ", ".join(set_clauses)
    query += " WHERE id = :collection_id AND project_id = :project_id"
    values["collection_id"] = collection_id
    values["project_id"] = project_id

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Execute the update statement
                statement = text(query).bindparams(**values)
                result = connection.execute(statement)

                # Check if any rows were affected
                if result.rowcount < 1:
                    return jsonify({"msg": f"No publication collection with id {collection_id} exists or no changes were made."}), 404

                return jsonify({"msg": f"Publication collection with id {collection_id} updated successfully."}), 200

    except Exception as e:
        result = {
            "msg": f"Failed to update publication collection with id {collection_id}.",
            "reason": str(e)
        }
        return jsonify(result), 500


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
    connection = db_engine.connect()
    with connection.begin():
        query = select(collections.c.publication_collection_introduction_id).where(collections.c.id == int_or_none(collection_id))
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
        with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(collections.c.publication_collection_title_id).where(collections.c.id == int_or_none(collection_id))
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
        with connection.begin():
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
    Edit a publication in the specified project by updating its fields.

    Parameters:
    - project (str): The name of the project.
    - publication_id (str): The ID of the publication to be updated. Must be a valid integer.

    Optional POST data parameters in JSON format (at least one required):
    - publication_collection_id (int): The ID of the publication collection. Must be a positive integer.
    - publication_comment_id (int): The ID of the publication comment. Must be a positive integer.
    - name (str): The name/title of the publication.
    - original_filename (str): Path to the publication’s XML-file in the project GitHub repository.
    - original_publication_date (str): The original publication date or year (formatted as a string).
    - published (int): The publication status. Must be an integer with values 0, 1, or 2.
    - language (str): The language code of the publication (ISO 639-1).
    - genre (str): The genre of the publication.
    - deleted (int): Soft delete flag. Must be an integer with values 0 or 1.

    Additionally, all POST data parameter values can be set to null, except 'deleted'.

    Returns:
        JSON: A success message if the publication was updated, or an error message.

    Example Request:
        POST /projectname/publication/123/edit/
        Body:
        {
            "name": "New Publication Name",
            "published": 1,
            "language": "en"
        }

    Example Response (Success):
        {
            "msg": "Publication with id 123 updated successfully."
        }

    Example Response (Error):
        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:
        200 - OK: The publication was updated successfully.
        400 - Bad Request: Invalid publication_id, invalid field values, or no valid fields provided for the update.
        404 - Not Found: No publication with the given publication_id exists or no changes were made.
        500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that publication_id is an integer
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # List of fields to check in request_data
    fields = ["publication_collection_id",
              "publication_comment_id",
              "name",
              "original_filename",
              "original_publication_date",
              "published",
              "language",
              "genre",
              "deleted"]

    # Start building the update query
    query = "UPDATE publication SET "
    values = {}
    set_clauses = []

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                set_clauses.append(f"{field} = NULL")
            else:
                # Validate integer field values and ensure all other fields are strings
                if field in ["publication_collection_id", "publication_comment_id"]:
                    if not isinstance(request_data[field], int) or request_data[field] < 1:
                        return jsonify({"msg": f"Field '{field}' must be a positive integer."}), 400
                elif field == "published":
                    if not isinstance(request_data[field], int) or request_data[field] < 0 or request_data[field] > 2:
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
                elif field == "deleted":
                    if not isinstance(request_data[field], int) or request_data[field] < 0 or request_data[field] > 1:
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the set_clauses and values for the query
                set_clauses.append(f"{field} = :{field}")
                values[field] = request_data[field]

    if not set_clauses:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified field to SET clauses
    set_clauses.append("date_modified = :date_modified")
    values["date_modified"] = datetime.now()

    # Join all SET clauses with commas
    query += ", ".join(set_clauses)
    query += " WHERE id = :publication_id"
    values["publication_id"] = publication_id

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Execute the update statement
                statement = text(query).bindparams(**values)
                result = connection.execute(statement)

                # Check if any rows were affected
                if result.rowcount < 1:
                    return jsonify({"msg": f"No publication with id {publication_id} exists or no changes were made."}), 404

                return jsonify({"msg": f"Publication with id {publication_id} updated successfully."}), 200

    except Exception as e:
        # Handle errors and return error response
        result = {
            "msg": f"Failed to update publication with id {publication_id}.",
            "reason": str(e)
        }
        return jsonify(result), 500


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
    connection = db_engine.connect()
    with connection.begin():
        query = select(publications.c.publication_comment_id).where(publications.c.id == int_or_none(publication_id))
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
            with connection.begin():
                update = comments.update().where(comments.c.id == int(comment_id)).values(**values)
                connection.execute(update)
            connection.close()
            return jsonify({
                "msg": "Updated comment {} with values {}".format(comment_id, str(values)),
                "comment_id": comment_id
            })
        else:
            with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(publications).where(publications.c.id == int_or_none(publication_id))
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
    with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(manuscripts).where(manuscripts.c.id == int_or_none(manuscript_id))
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
        with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(publications).where(publications.c.id == int_or_none(publication_id))
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

    with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(versions).where(versions.c.id == int_or_none(version_id))
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
        with connection.begin():
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
    connection = db_engine.connect()
    with connection.begin():
        query = select(collections).where(collections.c.id == int_or_none(collection_id))
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
        with connection.begin():
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
    else:
        collection_result = collection_result._asdict()

    intro_id = int_or_none(collection_result["publication_collection_introduction_id"])
    title_id = int_or_none(collection_result["publication_collection_title_id"])
    intro_query = select(intros.c.published, intros.c.original_filename).where(intros.c.id == intro_id)
    title_query = select(titles.c.published, titles.c.original_filename).where(titles.c.id == title_id)

    intro_result = connection.execute(intro_query).fetchone()._asdict()
    title_result = connection.execute(title_query).fetchone()._asdict()

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
