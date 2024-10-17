import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import select
from datetime import datetime

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, get_table, int_or_none, \
    project_permission_required, validate_project_name, validate_int


publishing_tools = Blueprint("publishing_tools", __name__)

logger = logging.getLogger("sls_api.tools.publishing")


@publishing_tools.route("/projects/new/", methods=["POST"])
@jwt_required()
def add_new_project():
    """
    Create a new project.

    POST Data Parameters in JSON Format:

    - name (str, required): The name/title of the new project. The name
      can only contain lowercase letters (a-z), digits (0-9) and
      underscores (_), and must be between 3 and 32 characters long
      (inclusive). The project name must be unique.
    - published (int): The published status of the project.
      Must be an integer with value 0, 1 or 2. Defaults to 1.

    Returns:

        JSON: A success message with the inserted row, or an error message.

    Example Request:

        POST /projects/new/
        Body:
        {
            "name": "My New Project"
        }

    Example Response (Success):

        {
            "msg": "Created new project.",
            "row": {
                "id": 123,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": null,
                "deleted": 0,
                "published": 1,
                "name": "My New Project"
            }
        }

    Example Response (Error):

        {
            "msg": "No data provided."
        }

    Status Codes:

    - 201 - Created: The project was inserted successfully.
    - 400 - Bad Request: No data was provided in the request,
            or required fields are missing.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # Validate request data and construct dict with insert values
    values = {}
    if "name" not in request_data:
        return jsonify({"msg": "Project name required."}), 400

    name = str(request_data.get("name"))

    # Validate project name
    is_valid_name, name_error_msg = validate_project_name(name)
    if not is_valid_name:
        return jsonify({"msg": name_error_msg}), 400

    values["name"] = name

    if "published" in request_data:
        if not validate_int(request_data["published"], 0, 2):
            return jsonify({"msg": "Field 'published' must be an integer with value 0, 1 or 2."}), 400
        else:
            values["published"] = request_data["published"]
    else:
        values["published"] = 1

    project_table = get_table("project")

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Check for existing project with the same name
                select_stmt = (
                    select(project_table.c.id)
                    .where(project_table.c.name == name)
                )
                result = connection.execute(select_stmt).first()

                if result:
                    return jsonify({"msg": "A project with this name already exists."}), 400

                # Proceed to insert the new project
                insert_stmt = (
                    project_table.insert()
                    .values(**values)
                    .returning(*project_table.c)  # Return the inserted row
                )
                result = connection.execute(insert_stmt)
                inserted_row = result.fetchone()  # Fetch the inserted row

                if inserted_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Insertion failed: no row returned.",
                        "reason": "The insert statement did not return any data."
                    }), 500

                # Convert the inserted row to a dict for JSON serialization
                inserted_row_dict = inserted_row._asdict()

                return jsonify({
                    "msg": f"New project with ID {inserted_row['id']} created successfully.",
                    "row": inserted_row_dict
                }), 201

    except Exception as e:
        return jsonify({"msg": "Failed to create new project.",
                        "reason": str(e)}), 500


@publishing_tools.route("/projects/<project_id>/edit/", methods=["POST"])
@jwt_required()
def edit_project(project_id):
    """
    Edit fields of the specified project.

    URL Path Parameters:

    - project_id (int, required): The ID of the project to edit.

    POST Data Parameters in JSON Format (at least one required):

    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
    - published (int): The publication status of the project.
      Must be an integer with value 0, 1 or 2.

    Returns:

        JSON: A message indicating the result of the operation and the
        updated project row, or an error message.

    Example Request:

        POST /projects/123/edit/
        Body:
        {
            "deleted": 0,
            "published": 1
        }

    Example Response (Success):

        {
            "msg": "Updated project with ID 123 successfully.",
            "row": {
                "id": 123,
                "date_created": "2023-01-01T10:00:00",
                "date_modified": "2023-10-17T12:34:56",
                "deleted": 0,
                "published": 1,
                "name": "projectname"
            }
        }

    Example Response (Error):

        {
            "msg": "Invalid project_id, must be a positive integer."
        }

    Status Codes:

    - 200: The project was successfully updated.
    - 400 - Bad Request: Invalid `project_id`, field values or no data provided.
    - 404 - Not Found: No project exists with the specified `project_id`.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Convert project_id to integer and verify
    project_id = int_or_none(project_id)
    if not project_id or project_id < 1:
        return jsonify({"msg": "Invalid project_id, must be a positive integer."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # List allowed fields in POST data
    fields = ["deleted", "published"]

    # Verify that POST data contains at least one valid field
    if all(field not in request_data for field in fields):
        return jsonify({"msg": "POST data contains no valid fields."}), 400

    # Start building values dictionary for update statement
    values = {}

    # Loop over all fields and validate them
    for field in fields:
        if field in request_data:
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the field_names list for the query construction
            values[field] = request_data[field]

    if values:
        values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                project_table = get_table("project")
                stmt = (
                    project_table.update()
                    .where(project_table.c.id == project_id)
                    .values(**values)
                    .returning(*project_table.c)  # Return the updated row
                )
                result = connection.execute(stmt)
                updated_row = result.fetchone()  # Fetch the updated row

                if updated_row is None:
                    # No row was returned; project_id invalid
                    return jsonify({"msg": "Invalid project_id."}), 404

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return jsonify({
                    "msg": f"Updated project with ID {project_id} successfully.",
                    "row": updated_row_dict
                })

    except Exception as e:
        return jsonify({"msg": "Failed to update project.",
                        "reason": str(e)}), 500


@publishing_tools.route("/<project>/publication_collection/<collection_id>/edit/", methods=["POST"])
@project_permission_required
def edit_publication_collection(project, collection_id):
    """
    Edit a publication collection in the specified project by updating
    it’s fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - collection_id (str): The id of the publication collection to be updated.
      Must be a valid positive integer.

    POST Data Parameters in JSON Format (at least one required):

    - name (str or null): The name/title of the publication collection.
    - published (int): The publication status. Must be an integer with
      value 0, 1 or 2.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.

    Returns:

        JSON: A success message with the updated publication collection row,
        or an error message.

    Example Request:

        POST /projectname/publication_collection/456/edit/
        Body:
        {
            "name": "Updated Collection Name",
            "published": 1
        }

    Example Response (Success):

        {
            "msg": "Publication collection with id 456 updated successfully.",
            "row": {
                "id": 456,
                "publication_collection_introduction_id": null,
                "publication_collection_title_id": null,
                "project_id": 4,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": "2023-08-14T14:29:02",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "name": "Updated Collection Name",
                "legacy_id": null,
                "name_translation_id": 4297
            }
        }

    Example Response (Error):

        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:

    - 200 - OK: The publication collection was updated successfully.
    - 400 - Bad Request: Invalid collection_id, invalid field values,
            or no valid fields provided for the update.
    - 404 - Not Found: No publication collection with the given collection_id
            exists or no changes were made.
    - 500 - Internal Server Error: Database query or execution failed.
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

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other
            # fields are strings or None
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the values list for the query construction
            values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                collection_table = get_table("publication_collection")
                upd_stmt = (
                    collection_table.update()
                    .where(collection_table.c.id == collection_id)
                    .where(collection_table.c.project_id == project_id)
                    .values(**values)
                    .returning(*collection_table.c)  # Return the updated row
                )
                result = connection.execute(upd_stmt)
                updated_row = result.fetchone()  # Fetch the updated row

                if updated_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": f"Update failed: No publication collection with ID {collection_id} exists in project with ID {project_id} or no changes were made."
                    }), 404

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return jsonify({
                    "msg": f"Publication collection with ID {collection_id} updated successfully.",
                    "row": updated_row_dict
                })

    except Exception as e:
        result = {
            "msg": f"Failed to update publication collection with ID {collection_id}.",
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
    Edit a publication in the specified project by updating it's fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - publication_id (str): The ID of the publication to be updated.
      Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - publication_collection_id (int): The ID of the publication collection.
      Must be a positive integer.
    - publication_comment_id (int): The ID of the publication comment.
      Must be a positive integer.
    - name (str): The name/title of the publication.
    - original_filename (str): Path to the publication’s XML-file in the
      project GitHub repository.
    - original_publication_date (str): The original publication date or
      year (formatted as a string).
    - published (int): The publication status. Must be an integer with
      value 0, 1 or 2.
    - language (str): The language code of the main language of the
      publication (ISO 639-1).
    - genre (str): The genre of the publication.
    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.

    Additionally, all POST data parameter values can be set to null, except 'deleted'.

    Returns:

        JSON: A success message and the updated row if the publication was
        updated, or an error message.

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
            "msg": "Publication with id 123 updated successfully.",
            "row": {
                "id": 123,
                "publication_collection_id": 585,
                "publication_comment_id": 5487,
                "date_created": "2023-05-12T12:34:56",
                "date_modified": "2023-06-01T08:22:11",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "/path/to/file.xml",
                "name": "New Publication Name",
                "genre": "non-fiction",
                "publication_group_id": null,
                "original_publication_date": "1854",
                "zts_id": null,
                "language": "en"
            }
        }

    Example Response (Error):

        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:

    - 200 - OK: The publication was updated successfully.
    - 400 - Bad Request: Invalid publication_id, invalid field values,
            or no valid fields provided for the update.
    - 404 - Not Found: No publication with the given publication_id exists
            or no changes were made.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

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

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            if request_data[field] is None and field != "deleted":
                values[field] = None
            else:
                # Validate integer field values and ensure all other
                # fields are strings
                if field in ["publication_collection_id", "publication_comment_id"]:
                    if not validate_int(request_data[field], 1):
                        return jsonify({"msg": f"Field '{field}' must be a positive integer."}), 400
                elif field == "published":
                    if not validate_int(request_data[field], 0, 2):
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
                elif field == "deleted":
                    if not validate_int(request_data[field], 0, 1):
                        return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
                else:
                    # Convert remaining fields to string
                    request_data[field] = str(request_data[field])

                # Add the field to the values list for the query construction
                values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                # Verify publication_id and that the publication is
                # in the project
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                stmt = (
                    select(publication_table.c.id)
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == publication_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return jsonify({"msg": "Publication not found in project. Either project name or publication_id is invalid."}), 404

                # Execute the update statement
                upd_stmt = (
                    publication_table.update()
                    .where(publication_table.c.id == publication_id)
                    .values(**values)
                    .returning(*publication_table.c)  # Return the updated row
                )
                result = connection.execute(upd_stmt)
                updated_row = result.fetchone()  # Fetch the updated row

                if updated_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Update failed: no row returned.",
                        "reason": "The update statement did not return any data."
                    }), 404

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return jsonify({
                    "msg": f"Updated publication with ID {publication_id} successfully.",
                    "row": updated_row_dict
                })

    except Exception as e:
        # Handle errors and return error response
        result = {
            "msg": f"Failed to update publication with ID {publication_id}.",
            "reason": str(e)
        }
        return jsonify(result), 500


@publishing_tools.route("/<project>/publication/<publication_id>/comment/edit/", methods=["POST"])
@project_permission_required
def edit_comment(project, publication_id):
    """
    Edit a comment of the specified publication in the given project by updating its fields.

    URL Path Parameters:

    - project (str): The name of the project.
    - publication_id (str): The ID of the publication whose comment is
      to be updated. Must be a valid integer.

    POST Data Parameters in JSON Format (at least one required):

    - deleted (int): Soft delete flag. Must be an integer with value 0 or 1.
    - published (int): The publication status of the comment. Must be an
      integer with value 0, 1, or 2.
    - original_filename (str): Path to the comment’s XML-file in the
      project GitHub repository.

    Returns:

        JSON: A success message and the updated comment data if the
        comment was updated, or an error message.

    Example Request:

        POST /projectname/publication/123/comment/edit/
        Body:
        {
            "published": 1,
            "deleted": 0,
            "original_filename": "path/to/updated_comment_file.xml"
        }

    Example Response (Success):

        {
            "msg": "Updated comment of publication with ID 123 successfully.",
            "row": {
                "id": 456,
                "date_created": "2023-07-12T09:23:45",
                "date_modified": "2023-08-14T14:29:02",
                "date_published_externally": null,
                "deleted": 0,
                "published": 1,
                "legacy_id": null,
                "published_by": null,
                "original_filename": "path/to/updated_comment_file.xml"
            }
        }

    Example Response (Error):

        {
            "msg": "Field 'published' must be an integer with value 0, 1 or 2."
        }

    Status Codes:

    - 200 - OK: The comment was updated successfully.
    - 400 - Bad Request: Invalid publication_id, invalid field values,
            no data provided, or no valid fields provided to update.
    - 404 - Not Found: Publication not found in project, or comment
            linked to publication not found.
    - 500 - Internal Server Error: Database query or execution failed.
    """
    # Verify that project name is valid and get project_id
    project_id = get_project_id_from_name(project)
    if not project_id:
        return jsonify({"msg": "Invalid project name."}), 400

    # Verify that publication_id is an integer
    publication_id = int_or_none(publication_id)
    if not publication_id or publication_id < 1:
        return jsonify({"msg": "Invalid publication_id."}), 400

    # Verify that request data was provided
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    # List of fields to check in request_data
    fields = ["deleted", "published", "original_filename"]

    # Start building values dictionary for update statement
    values = {}

    # Loop over the fields list and check each one in request_data
    for field in fields:
        if field in request_data:
            # Validate integer field values and ensure all other
            # fields are strings or None
            if field == "published":
                if not validate_int(request_data[field], 0, 2):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0, 1 or 2."}), 400
            elif field == "deleted":
                if not validate_int(request_data[field], 0, 1):
                    return jsonify({"msg": f"Field '{field}' must be an integer with value 0 or 1."}), 400
            else:
                # Convert remaining fields to string if not None
                if request_data[field] is not None:
                    request_data[field] = str(request_data[field])

            # Add the field to the values list for the query construction
            values[field] = request_data[field]

    if not values:
        return jsonify({"msg": "No valid fields provided to update."}), 400

    # Add date_modified
    values["date_modified"] = datetime.now()

    try:
        with db_engine.connect() as connection:
            with connection.begin():
                collection_table = get_table("publication_collection")
                publication_table = get_table("publication")
                comment_table = get_table("publication_comment")

                # Get publicatiom_comment_id
                stmt = (
                    select(publication_table.c.publication_comment_id)
                    .join(collection_table, publication_table.c.publication_collection_id == collection_table.c.id)
                    .where(collection_table.c.project_id == project_id)
                    .where(publication_table.c.id == publication_id)
                )
                result = connection.execute(stmt).first()

                if result is None:
                    return jsonify({"msg": "Publication not found in project. Either project name or publication_id is invalid."}), 404

                com_id = result["publication_comment_id"]

                # Execute the update statement
                upd_stmt = (
                    comment_table.update()
                    .where(comment_table.c.id == com_id)
                    .values(**values)
                    .returning(*comment_table.c)  # Return the updated row
                )
                result = connection.execute(upd_stmt)
                updated_row = result.fetchone()  # Fetch the updated row

                if updated_row is None:
                    # No row was returned; handle accordingly
                    return jsonify({
                        "msg": "Update failed: could not find comment linked to publication."
                    }), 404

                # Convert the inserted row to a dict for JSON serialization
                updated_row_dict = updated_row._asdict()

                return jsonify({
                    "msg": f"Updated comment of publication with ID {publication_id} successfully.",
                    "row": updated_row_dict
                })

    except Exception as e:
        # Handle errors and return error response
        result = {
            "msg": f"Failed to update comment for publication with ID {publication_id}.",
            "reason": str(e)
        }
        return jsonify(result), 500


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
