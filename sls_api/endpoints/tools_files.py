import base64
from flask import Blueprint, jsonify, request, safe_join
from flask_jwt_extended import get_jwt_identity
import io
import logging
import os
import subprocess
import svn.local
import svn.remote
from werkzeug.utils import secure_filename

from sls_api.endpoints.generics import web_files_config, master_config, project_permission_required


file_tools = Blueprint("file_tools", __name__)
logger = logging.getLogger("tools_files")

svn_remotes = {}
svn_locals = {}


def check_project_web_repo_config(project):
    """
    Check the config file for project webfiles repository configuration.
    Returns True if config okay, otherwise False and a message
    """
    if project not in web_files_config:
        return False, "Project config not found."
    if not is_a_test(project) and "git_repository" not in web_files_config[project]:
        return False, "git_repository not in project config."
    if "git_branch" not in web_files_config[project]:
        return False, "git_branch information not in project config."
    if "file_root" not in web_files_config[project]:
        return False, "file_root information not in project config."
    return True, "Project config OK."


def file_exists_in_web_repo_root(project, file_path):
    """
    Check if the given file exists in the webfiles repository for the given project
    Returns True if the file exists, otherwise False.
    """
    return os.path.exists(safe_join(web_files_config[project]["file_root"], file_path))


def file_exists_in_master_repo_foot(project, file_path):
    """
    Check if the given file exists in the master file repository for the given project
    Returns True if the file exists, otherwise False.
    """
    return os.path.exists(safe_join(master_config[project]["file_root"], file_path))


def run_web_repo_command(project, command):
    """
    Helper method to run arbitrary git commands as if in the project's webfiles repository root folder
    @type project: str
    @type command: list
    """
    git_root = web_files_config[project]["file_root"]
    git_command = ["git", "-C", git_root]
    for c in command:
        git_command.append(c)
    return subprocess.check_output(git_command)


@file_tools.before_app_first_request
def initialize_master_repos():
    """
    Initializes the SVN master repositories by checking out their SVN remotes to the configured file_roots
    """
    global svn_remotes
    global svn_locals
    for project in master_config:
        if "svn_remote" in master_config[project]:
            try:
                svn_remotes[project] = svn.remote.RemoteClient(master_config[project]["svn_remote"],
                                                               username=master_config[project]["username"],
                                                               password=master_config[project]["password"])

                svn_remotes[project].checkout(master_config[project]["file_root"])
                svn_locals[project] = svn.local.LocalClient(master_config[project]["file_root"],
                                                            username=master_config[project]["username"],
                                                            password=master_config[project]["password"])
                logger.info("Initialized SVN working copy for master_files of project {} at file_root {!r}".format(project, master_config[project]["file_root"]))
            except Exception:
                logger.exception("Failed to initialize SVN repository for project {}".format(project))
                svn_remotes[project] = None
                svn_locals[project] = None
        else:
            logger.warning("svn_remote missing from master_files config for project {}, skipping SVN initialization...".format(project))
            svn_remotes[project] = None
            svn_locals[project] = None


def update_files_in_web_repo(project, specific_file=False):
    """
    Helper method to sync local repositories with remote to get latest changes
    """
    git_branch = web_files_config[project]["git_branch"]

    # First, fetch latest changes from remote, but don't update local
    try:
        run_web_repo_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return False, str(e.output)

    if not specific_file:
        # If we're updating all files, get the list of changed files and then merge in remote changes to local repo
        try:
            output = run_web_repo_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(git_branch)])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        try:
            run_web_repo_command(project, ["merge", "origin/{}".format(git_branch)])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, new_and_changed_files
    else:
        # If we're only updating one file, checkout that specific file, ignoring the others
        # This makes things go faster if we're not concerned with the changes in other files at the moment
        try:
            run_web_repo_command(project, ["checkout", "origin/{}".format(git_branch), "--", specific_file])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, specific_file


@file_tools.route("/<project>/sync_files_from_remote/web", methods=["POST"])
@project_permission_required
def pull_web_repo_changes_from_remote(project):
    """
    Sync API's local repo with the git remote, ensuring that all files are updated to their latest versions
    """
    # verify git config
    config_okay = check_project_web_repo_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    sync_repo = update_files_in_web_repo(project)

    # TODO merge conflict handling

    if sync_repo[0]:
        return jsonify({
            "msg": "Git repository successfully synced for project {}".format(project),
            "changed_files": sync_repo[1]
        })
    else:
        return jsonify({
            "msg": "Git update failed to execute properly.",
            "reason": sync_repo[1]
        }), 500


@file_tools.route("/<project>/sync_files_from_remote/master", methods=["POST"])
@project_permission_required
def pull_master_repo_changes_from_remote(project):
    """
    Sync API's local working copy with the SVN remote, ensuring that all master_files are updated to their latest versions
    Returns SVN repo status in addition to msg and status code.
    """
    if svn_locals.get(project, None) is not None:
        try:
            svn_locals[project].update()
            return jsonify({
                "msg": "SVN update successful.",
                "status": svn_locals[project].status()
            })
        except Exception:
            logging.exception("Failed to fetch updates from SVN master for project {}".format(project))
            return jsonify({
                "msg": "SVN update failed.",
                "status": svn_locals[project].status()
            }), 500
    else:
        return jsonify({
            "msg": "No SVN remote configured for project {}.".format(project)
        }), 500


def is_a_test(project):
    """
    Returns true if running in debug mode and project git_repository not configured, indicating that this is a test
    """
    if web_files_config[project]["git_repository"] is None and int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        return True


@file_tools.route("/<project>/update_file/by_path/web/<path:file_path>", methods=["PUT"])
@project_permission_required
def update_file_in_web_repo(project, file_path):
    """
    Add new or update existing file in git remote.

    PUT data MUST be in JSON format

    PUT data MUST contain the following:
    file: xml file data in base64, to be created or updated in git repository

    PUT data MAY contain the following override information:
    author: email of the person authoring this change, if not given, JWT identity is used instead
    message: commit message for this change, if not given, generic "File update by <author>" message is used instead
    force: boolean value, if True uses force-push to override errors and possibly mangle the git remote to get the update through
    """

    # Check if request has valid JSON and set author/message/force accordingly
    request_data = request.get_json()
    if not request_data:
        return jsonify({"msg": "No JSON in PUT request."}), 400
    elif "file" not in request_data:
        return jsonify({"msg": "No file in JSON data."}), 400

    author_email = request_data.get("author", get_jwt_identity()["sub"])
    message = request_data.get("message", "File update by {}".format(author_email))
    force = bool(request_data.get("force", False))

    # git commit requires author info to be in the format "Name <email>"
    # As we only have an email address to work with, split email on @ and give first part as name
    # - foo@bar.org becomes "foo <foo@bar.org>"
    author = "{} <{}>".format(
        author_email.split("@")[0],
        author_email
    )

    # Read the file from request and decode the base64 string into raw binary data
    file = io.BytesIO(base64.b64decode(request_data["file"]))

    # verify git config
    config_okay = check_project_web_repo_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    # fetch latest changes from remote
    if not is_a_test(project):
        try:
            run_web_repo_command(project, ["fetch"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git fetch failed to execute properly.",
                "reason": str(e.output)
            }), 500

        # check if desired file has changed in remote since last update
        # if so, fail and return both user file and repo file to user, unless force=True
        try:
            output = run_web_repo_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(web_files_config[project]["git_branch"])])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git show failed to execute properly.",
                "reason": str(e.output)
            }), 500
        if safe_join(web_files_config[project]["file_root"], file_path) in new_and_changed_files and not force:
            with io.open(safe_join(web_files_config[project]["file_root"], file_path), mode="rb") as repo_file:
                file_bytestring = base64.b64encode(repo_file.read())
                return jsonify({
                    "msg": "File {} has been changed in git repository since last update, please manually check file changes.",
                    "your_file": request_data["file"],
                    "repo_file": file_bytestring.decode("utf-8")
                }), 409

        # merge in latest changes so that the local repository is updated
        try:
            run_web_repo_command(project, ["merge", "origin/{}".format(web_files_config[project]["git_branch"])])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git merge failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # check the status of the git repo, so we know if we need to git add later
    file_exists = file_exists_in_web_repo_root(project, file_path)

    # Secure filename and save new file to local repo
    filename = secure_filename(file_path)
    if file and filename:
        with io.open(filename, mode="wb") as new_file:
            new_file.write(file.getvalue())

    # Add file to local repo if it wasn't already in the repository
    if not file_exists:
        try:
            run_web_repo_command(project, ["add", filename])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git add failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # Commit changes to local repo, noting down user and commit message
    try:
        run_web_repo_command(project, ["commit", "--author={}".format(author), "-m", message])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git commit failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # push new commit to remote repository
    if not is_a_test(project):
        try:
            if force:
                run_web_repo_command(project, ["push", "-f"])
            else:
                run_web_repo_command(project, ["push"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git push failed to execute properly.",
                "reason": str(e.output)
            }), 500

    return jsonify({
        "msg": "File updated successfully in repository."
    })


@file_tools.route("/<project>/update_file/by_path/master/<path:file_path>", methods=["PUT"])
@project_permission_required
def update_file_in_master_repo(project, file_path):
    # TODO
    # Merge conflicts handled by parameters
    # parameter "accept" for --accept working
    # parameter "discard" for --accept theirs-full ? possibly just svn revert?
    # Only commit if no conflicts
    pass


@file_tools.route("/<project>/get_latest_file/by_path/web/<path:file_path>")
@project_permission_required
def get_file_from_web_repo(project, file_path):
    """
    Get latest file from git remote
    """
    # TODO swift and/or S3 support for large files (images/facsimiles)
    config_okay = check_project_web_repo_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    if not is_a_test(project):
        # Sync the desired file from remote repository to local API repository
        update_repo = update_files_in_web_repo(project, file_path)
        if not update_repo[0]:
            return jsonify({
                "msg": "Git update failed to execute properly.",
                "reason": update_repo[1]
            }), 500

    if file_exists_in_web_repo_root(project, file_path):
        # read file, encode as base64 string and return to user as JSON data.
        with io.open(safe_join(web_files_config[project]["file_root"], file_path), mode="rb") as file:
            file_bytestring = base64.b64encode(file.read())
            return jsonify({
                "file": file_bytestring.decode("utf-8"),
                "filepath": file_path
            })
    else:
        return jsonify({"msg": "The requested file was not found in the git repository."}), 404


@file_tools.route("/<project>/get_latest_file/by_path/master/<path:file_path>")
@project_permission_required
def get_file_from_master_repo(project, file_path):
    # TODO
    pass


@file_tools.route("/<project>/get_tree/web/")
@file_tools.route("/<project>/get_tree/web/<path:file_path>")
@project_permission_required
def get_file_tree_from_web_repo(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    # Fetch changes (to update index) but don't merge, and then run ls-files to get file listing.
    try:
        if not is_a_test(project):
            run_web_repo_command(project, ["fetch"])
        if file_path is None:
            output = run_web_repo_command(project, ["ls-files"])
        else:
            output = run_web_repo_command(project, ["ls-files", file_path])
        file_listing = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git file listing failed.",
            "reason": str(e.output)
        }), 500
    tree = path_list_to_tree(file_listing)
    return jsonify(tree)


@file_tools.route("/<project>/get_tree/web/")
@file_tools.route("/<project>/get_tree/web/<path:file_path>")
@project_permission_required
def get_file_tee_from_master_repo(project, file_path=None):
    # TODO
    pass


def path_list_to_tree(path_list):
    """
    Turn a list of filepaths into a nested dict
    """
    file_tree = {}
    for path in path_list:
        _recurse(path, file_tree)
    return file_tree


def _recurse(path, container):
    """
    Recurse over path and container to make a nested dict of path in container
    """
    parts = path.split("/")
    head = parts[0]
    tail = parts[1:]
    if not tail:
        container[head] = None
    else:
        if head not in container:
            container[head] = {}
        _recurse("/".join(tail), container[head])
