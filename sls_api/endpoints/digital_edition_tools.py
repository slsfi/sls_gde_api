from flask import Blueprint, request
from flask_jwt_extended import jwt_required
import subprocess

de_tools = Blueprint("digital_edition_tools", __name__)


@de_tools.route("/<project>/new_location", methods=["POST"])
def add_new_location(project):
    """
    Add a new location object to the database
    """
    pass


@de_tools.route("/<project>/new_subject", methods=["POST"])
def add_new_subject(project):
    """
    Add a new subject object to the database
    """
    pass


@de_tools.route("/<project>/new_tag", methods=["POST"])
def add_new_tag(project):
    """
    Add a new tag object to the database
    """
    pass


@de_tools.route("/<project>/new_event", methods=["POST"])
def add_new_event(project):
    """
    Add a new eventOccurance, event, and evnetConnection to the database for a location, subject, or tag
    """
    pass


@de_tools.route("/update_xml/<project>/by_path/<file_path>", methods=["POST", "UPDATE"])
def update_file_in_remote(project, file_path):
    """
    Add new XML or update existing XML in git remote
    """
    pass


@de_tools.route("/get_latest/<project>/by_path/<file_path>")
def get_file_from_remote(project, file_path):
    """
    Get latest XML file from git remote
    """
    pass


@de_tools.route("/<project>/get_tree/")
@de_tools.route("/<project>/get_tree/<file_path>")
def get_file_tree_from_remote(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    pass


@de_tools.route("/<project>/fascimile_collection/new", methods=["POST"])
def create_fascimile_collection(project):
    """
    Create a new publicationFascimileCollection
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list")
def list_fascimile_collections(project):
    """
    List all available publicationFascimileCollections
    """
    pass


@de_tools.route("/<project>/fascimile_collection/link")
def link_fascimile_collection_to_publication(project):
    """
    Link a publicationFascimileCollection to a publication through publicationFascimile table
    """
    pass


@de_tools.route("/<project>/fascimile_collection/list_links")
def list_fascimile_collection_links(project):
    """
    List all links between a publicationFascimileCollection and its publicationFascimile objects
    """
    pass
