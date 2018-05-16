from flask import Blueprint, request
from flask_jwt_extended import jwt_required
from ruamel.yaml import YAML
import subprocess

de_tools = Blueprint("digital_edition_tools", __name__)


@de_tools.route("/<project>/new_location", methods=["POST"])
@jwt_required
def add_new_location(project):
    """
    Add a new location object to the database
    """
    pass


@de_tools.route("/<project>/new_subject", methods=["POST"])
@jwt_required
def add_new_subject(project):
    """
    Add a new subject object to the database
    """
    pass


@de_tools.route("/<project>/new_tag", methods=["POST"])
@jwt_required
def add_new_tag(project):
    """
    Add a new tag object to the database
    """
    pass


@de_tools.route("/<project>/new_event", methods=["POST"])
@jwt_required
def add_new_event(project):
    """
    Add a new eventOccurance, event, and evnetConnection to the database for a location, subject, or tag
    """
    pass


@de_tools.route("/update_xml/<project>/by_path/<file_path>", methods=["POST", "UPDATE"])
@jwt_required
def update_file_in_remote(project, file_path):
    """
    Add new XML or update existing XML in git remote
    """
    pass


@de_tools.route("/get_latest/<project>/by_path/<file_path>")
@jwt_required
def get_file_from_remote(project, file_path):
    """
    Get latest XML file from git remote
    """
    pass


@de_tools.route("/<project>/get_tree/")
@de_tools.route("/<project>/get_tree/<file_path>")
@jwt_required
def get_file_tree_from_remote(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    pass


@de_tools.route("/<project>/fascimile_collection/new", methods=["POST"])
@jwt_required
def create_fascimile_collection(project):
    """
    Create a new publicationFascimileCollection
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list")
@jwt_required
def list_fascimile_collections(project):
    """
    List all available publicationFascimileCollections
    """
    pass


@de_tools.route("/<project>/fascimile_collection/link")
@jwt_required
def link_fascimile_collection_to_publication(project):
    """
    Link a publicationFascimileCollection to a publication through publicationFascimile table
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list_links")
@jwt_required
def list_fascimile_collection_links(project):
    """
    List all links between a publicationFascimileCollection and its publicationFascimile objects
    """
    pass


@de_tools.route("/projects/")
@jwt_required
def list_projects():
    """
    List all GDE projects
    """
    pass


@de_tools.route("/<project>/publication_collection/list")
@jwt_required
def list_publication_collections(project):
    """
    List all publicationCollection objects for a given project
    """
    pass


@de_tools.route("/<project>/publication_collection/new", methods=["POST"])
@jwt_required
def new_publication_collection(project):
    """
    Create a new publicationCollection object and associated Introduction and Title objects.
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/")
@jwt_required
def list_publications(project, collection_id):
    """
    List all publications in a given collection
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/<publication_id>")
@jwt_required
def get_publication(project, collection_id, publication_id):
    """
    Get a publication object from the database
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/new", methods=["POST"])
@jwt_required
def new_publication(project, collection_id):
    """
    Create a new publication object as part of the given publicationCollection
    """
    pass


@de_tools.route("/<project>/publication/<collection_id>/<publication_id>/link_file", methods=["POST"])
@jwt_required
def link_file_to_publication(project, collection_id, publication_id):
    """
    Link an XML file to a publication,
    creating the appropriate publicationComment, publicationManuscript, or publicationVersion object.
    """
    pass
