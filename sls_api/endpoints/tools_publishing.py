import logging
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from sqlalchemy import Table
from sqlalchemy.sql import insert, select, update, delete

from sls_api.endpoints.generics import db_engine, get_project_id_from_name, metadata, project_permission_required


publishing_tools = Blueprint("publishing_tools", __name__)

logger = logging.getLogger("sls_api.publishing_tools")


@publishing_tools.route("/projects/new", methods=["POST"])
@jwt_required
def add_new_project():
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No data provided."}), 400

    projects = Table("project", metadata, autoload=True, autoload_with=db_engine)
    connection = db_engine.connect()


@publishing_tools.route("/projects/<project_id>/edit", methods=["POST"])
@jwt_required
def edit_project(project_id):
    pass


@publishing_tools.route("/projects/<project_id>/", methods=["DELETE"])
@jwt_required
def delete_project(project_id):
    pass


@publishing_tools.route("/<project>/publication_collection/<collection_id>/edit", methods=["POST"])
@project_permission_required
def edit_publication_collection(project, collection_id):
    pass


@publishing_tools.route("/<project>/publication_collection/<collection_id>/", methods=["DELETE"])
@project_permission_required
def delete_publication_collection(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro")
@project_permission_required
def get_intro(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro/edit", methods=["POST"])
@project_permission_required
def edit_intro(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/intro", methods=["DELETE"])
@project_permission_required
def delete_intro(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/title")
@project_permission_required
def get_title(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/title/edit", methods=["POST"])
@project_permission_required
def edit_title(project, collection_id):
    pass


@publishing_tools.route("<project>/publication_collection/<collection_id>/title", methods=["DELETE"])
@project_permission_required
def delete_title(project, collection_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/edit", methods=["POST"])
@project_permission_required
def edit_publication(project, publication_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/", methods=["DELETE"])
@project_permission_required
def delete_publication(project, publication_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/comment/edit", methods=["POST"])
@project_permission_required
def edit_comment(project, publication_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/comment/", methods=["DELETE"])
@project_permission_required
def delete_comment(project, publication_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/manuscripts/new", methods=["POST"])
@project_permission_required
def add_manuscript(project, publication_id):
    pass


@publishing_tools.route("/<project>/manuscripts/<manuscript_id>/edit", methods=["POST"])
@project_permission_required
def edit_manuscript(project, manuscript_id):
    pass


@publishing_tools.route("/<project>/manuscripts/<manuscript_id>/", methods=["DELETE"])
@project_permission_required
def delete_manuscript(project, manuscript_id):
    pass


@publishing_tools.route("/<project>/publication/<publication_id>/versions/new", methods=["POST"])
@project_permission_required
def add_version(project, publication_id):
    pass


@publishing_tools.route("/<project>/versions/<version_id>/edit", methods=["POST"])
@project_permission_required
def edit_version(project, version_id):
    pass


@publishing_tools.route("/<project>/versions/<version_id>/", methods=["DELETE"])
@project_permission_required
def delete_version(project, version_id):
    pass


@publishing_tools.route("/<project>/facsimile_collection/<collection_id>/edit", methods=["POST"])
@project_permission_required
def edit_facsimile_collection(project, collection_id):
    pass


@publishing_tools.route("/<project>/facsimile_collection/<collection_id>/", methods=["DELETE"])
@project_permission_required
def delete_facsimile_collection(project, collection_id):
    pass
