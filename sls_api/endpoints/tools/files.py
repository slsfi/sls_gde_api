import base64
from flask import Blueprint, jsonify, request, safe_join
from flask_jwt_extended import get_jwt_identity
import io
import json
import logging
import os
import subprocess

from sls_api.endpoints.generics import get_project_config, project_permission_required


file_tools = Blueprint("file_tools", __name__)
logger = logging.getLogger("sls_api.tools.files")


def check_project_config(project):
    """
    Check the config file for project webfiles repository configuration.
    Returns True if config okay, otherwise False and a message
    """
    config = get_project_config(project)
    if config is None:
        return False, "Project config not found."
    if not is_a_test(project) and "git_repository" not in config:
        return False, "git_repository not in project config."
    if "git_branch" not in config:
        return False, "git_branch information not in project config."
    if "file_root" not in config:
        return False, "file_root information not in project config."
    return True, "Project config OK."


def file_exists_in_file_root(project, file_path):
    """
    Check if the given file exists in the webfiles repository for the given project
    Returns True if the file exists, otherwise False.
    """
    config = get_project_config(project)
    if config is None:
        return False
    return os.path.exists(safe_join(config["file_root"], file_path))


def run_git_command(project, command):
    """
    Helper method to run arbitrary git commands as if in the project's webfiles repository root folder
    @type project: str
    @type command: list
    """
    config = get_project_config(project)
    git_root = config["file_root"]
    git_command = ["git", "-C", git_root]
    for c in command:
        git_command.append(c)
    return subprocess.check_output(git_command, stderr=subprocess.STDOUT)


def update_files_in_git_repo(project, specific_file=False):
    """
    Helper method to sync local repositories with remote to get latest changes
    """
    config = get_project_config(project)
    if config is None:
        return False, "No such project."
    git_branch = config["git_branch"]

    # First, fetch latest changes from remote, but don't update local
    try:
        run_git_command(project, ["fetch"])
    except subprocess.CalledProcessError as e:
        return False, str(e.output)

    if not specific_file:
        # If we're updating all files, get the list of changed files and then merge in remote changes to local repo
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(git_branch)])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        try:
            run_git_command(project, ["merge", "origin/{}".format(git_branch)])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, new_and_changed_files
    else:
        # If we're only updating one file, checkout that specific file, ignoring the others
        # This makes things go faster if we're not concerned with the changes in other files at the moment
        try:
            run_git_command(project, ["checkout", "origin/{}".format(git_branch), "--", specific_file])
        except subprocess.CalledProcessError as e:
            return False, str(e.output)
        return True, specific_file


@file_tools.route("/<project>/config/get", endpoint='get_config_file')
def get_config_file(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        file_path = os.path.join(config["file_root"], "config.json")
        if not os.path.exists(file_path):
            return jsonify({})
        with open(file_path) as f:
            json_data = json.load(f)
        return jsonify(json_data)


@file_tools.route("/<project>/config/update", methods=["POST"], endpoint='update_config')
@project_permission_required
def update_config(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        request_data = request.get_json()
        file_path = os.path.join(config["file_root"], "config.json")
        with open(file_path, "w") as f:
            json.dump(request_data, f)
        return jsonify({"msg": "received"})


@file_tools.route("/<project>/sync_files/", methods=["POST"], endpoint='pull_changes_from_git_remote')
@project_permission_required
def pull_changes_from_git_remote(project):
    """
    Sync API's local repo with the git remote, ensuring that all files are updated to their latest versions
    """
    # verify git config
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    sync_repo = update_files_in_git_repo(project)

    # TODO merge conflict handling, if necessary. wait and see how things pan out - may not be an issue.

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


def is_a_test(project):
    """
    Returns true if running in debug mode and project git_repository not configured, indicating that this is a test
    """
    config = get_project_config(project)
    if config is None and int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        return True
    elif config is not None and config["git_repository"] is None and int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        return True


def git_commit_and_push_file(project, author, message, file_path, force=False):
    # verify git config
    config_okay = check_project_config(project)
    if not config_okay[0]:
        logger.error("Error in git config, check project configuration!")
        return False

    config = get_project_config(project)

    # fetch latest changes from remote
    if not is_a_test(project):
        try:
            run_git_command(project, ["fetch"])
        except subprocess.CalledProcessError:
            logger.exception("Git fetch failed to execute properly.")
            return False

        # check if desired file has changed in remote since last update
        # if so, fail and return both user file and repo file to user, unless force=True
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only",
                                               "..origin/{}".format(config["git_branch"])])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            logger.error("Git show failed to execute properly.")
            logger.error(str(e.output))
            return False

        if safe_join(config["file_root"], file_path) in new_and_changed_files and not force:
            logger.error("File {} has been changed in git repository since last update, please manually check file changes.".format(file_path))
            return False

        # merge in latest changes so that the local repository is updated
        try:
            run_git_command(project, ["merge", "origin/{}".format(config["git_branch"])])
        except subprocess.CalledProcessError as e:
            logger.error("Git merge failed to execute properly.")
            logger.error(str(e.output))
            return False

    # git add file
    try:
        run_git_command(project, ["add", file_path])
    except subprocess.CalledProcessError as e:
        logger.error("Git add failed to execute properly!")
        logger.error(str(e.output))
        return False

    # Commit changes to local repo, noting down user and commit message
    try:
        run_git_command(project, ["commit", "--author={}".format(author), "-m", message])
    except subprocess.CalledProcessError as e:
        logger.error("Git commit failed to execute properly.")
        logger.error(str(e.output))
    else:
        logger.info("git commit of {} succeeded".format(file_path))

    # push new commit to remote repository
    if not is_a_test(project):
        try:
            if force:
                run_git_command(project, ["push", "-f"])
            else:
                run_git_command(project, ["push"])
        except subprocess.CalledProcessError as e:
            logger.error("Git push failed to execute properly.")
            logger.error(str(e.output))
            return False
        else:
            logger.info("git push of {} succeeded".format(file_path))
    # if we reach this point, the file has been commited (and possibly pushed)
    return True


@file_tools.route("/<project>/update_file/by_path/<path:file_path>", methods=["PUT"], endpoint='update_file')
@project_permission_required
def update_file(project, file_path):
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
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
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
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    # fetch latest changes from remote
    if not is_a_test(project):
        try:
            run_git_command(project, ["fetch"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git fetch failed to execute properly.",
                "reason": str(e.output)
            }), 500

        # check if desired file has changed in remote since last update
        # if so, fail and return both user file and repo file to user, unless force=True
        try:
            output = run_git_command(project, ["show", "--pretty=format:", "--name-only", "..origin/{}".format(config["git_branch"])])
            new_and_changed_files = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git show failed to execute properly.",
                "reason": str(e.output)
            }), 500
        if safe_join(config["file_root"], file_path) in new_and_changed_files and not force:
            with io.open(safe_join(config["file_root"], file_path), mode="rb") as repo_file:
                file_bytestring = base64.b64encode(repo_file.read())
                return jsonify({
                    "msg": "File {} has been changed in git repository since last update, please manually check file changes.",
                    "your_file": request_data["file"],
                    "repo_file": file_bytestring.decode("utf-8")
                }), 409

        # merge in latest changes so that the local repository is updated
        try:
            run_git_command(project, ["merge", "origin/{}".format(config["git_branch"])])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git merge failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # check the status of the git repo, so we know if we need to git add later
    file_exists = file_exists_in_file_root(project, file_path)

    # Secure filename and save new file to local repo
    # Could be more secure...
    pos = file_path.find('.xml')
    if pos > 0:
        filename = safe_join(config["file_root"], file_path)
        if file and filename:
            with io.open(filename, mode="wb") as new_file:
                new_file.write(file.getvalue())
    else:
        return jsonify({
                "msg": "File path error"
            }), 500

    # Add file to local repo if it wasn't already in the repository
    if not file_exists:
        try:
            run_git_command(project, ["add", filename])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git add failed to execute properly.",
                "reason": str(e.output)
            }), 500

    # Commit changes to local repo, noting down user and commit message
    try:
        run_git_command(project, ["commit", "--author={}".format(author), "-m", message])
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git commit failed to execute properly.",
            "reason": str(e.output)
        }), 500

    # push new commit to remote repository
    if not is_a_test(project):
        try:
            if force:
                run_git_command(project, ["push", "-f"])
            else:
                run_git_command(project, ["push"])
        except subprocess.CalledProcessError as e:
            return jsonify({
                "msg": "Git push failed to execute properly.",
                "reason": str(e.output)
            }), 500

    return jsonify({
        "msg": "File updated successfully in repository."
    })


@file_tools.route("/<project>/get_file/by_path/<path:file_path>", endpoint='get_file')
@project_permission_required
def get_file(project, file_path):
    """
    Get latest file from git remote
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    # TODO swift and/or S3 support for large files (images/facsimiles)
    config_okay = check_project_config(project)
    if not config_okay[0]:
        return jsonify({
            "msg": "Error in git configuration, check configuration file.",
            "reason": config_okay[1]
        }), 500

    if not is_a_test(project):
        # Sync the desired file from remote repository to local API repository
        update_repo = update_files_in_git_repo(project, file_path)
        if not update_repo[0]:
            return jsonify({
                "msg": "Git update failed to execute properly.",
                "reason": update_repo[1]
            }), 500

    if file_exists_in_file_root(project, file_path):
        # read file, encode as base64 string and return to user as JSON data.
        with io.open(safe_join(config["file_root"], file_path), mode="rb") as file:
            file_bytestring = base64.b64encode(file.read())
            return jsonify({
                "file": file_bytestring.decode("utf-8"),
                "filepath": file_path
            })
    else:
        return jsonify({"msg": "The requested file was not found in the git repository."}), 404


@file_tools.route("/<project>/get_tree/", endpoint='get_file_tree')
@file_tools.route("/<project>/get_tree/<path:file_path>", endpoint='get_file_tree')
@project_permission_required
def get_file_tree(project, file_path=None):
    """
    Get a file listing from the git remote
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    # Fetch changes (to update index) but don't merge, and then run ls-files to get file listing.
    try:
        if not is_a_test(project):
            run_git_command(project, ["fetch"])
        if file_path is None:
            output = run_git_command(project, ["ls-files"])
        else:
            output = run_git_command(project, ["ls-files", file_path])
        file_listing = [s.strip().decode('utf-8', 'ignore') for s in output.splitlines()]
    except subprocess.CalledProcessError as e:
        return jsonify({
            "msg": "Git file listing failed.",
            "reason": str(e.output)
        }), 500
    tree = path_list_to_tree(file_listing)
    return jsonify(tree)


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
