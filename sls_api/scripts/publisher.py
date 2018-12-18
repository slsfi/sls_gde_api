from lxml import etree
import os
from shutil import copy2 as copy_file
from sqlalchemy.sql import text
from subprocess import CalledProcessError
import traceback

from sls_api.endpoints.generics import config, db_engine, get_project_id_from_name
from sls_api.endpoints.tools_files import run_git_command


def generate_est_and_com_files(project, est_master_file_path, com_master_file_path, est_target_path, com_target_path):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    file_root = config[project]["file_root"]
    # TODO actual masterfile processing to webfiles using XSLT
    # TODO com file generation with both masterfiles and comments database
    copy_file(est_master_file_path, est_target_path)
    copy_file(com_master_file_path, com_target_path)


def generate_var_file(project, master_file_path, target_file_path):
    """
    Given a project name, and valid master and target file paths for a publication version, regenerates target file based on source file
    """
    file_root = config[project]["file_root"]
    copy_file(master_file_path, target_file_path)  # TODO actual masterfile processing to webfile using XSLT


def generate_ms_file(project, master_file_path, target_file_path):
    """
    Given a project name, and valid master and target file paths for a publication manuscript, regenerates target file based on source file
    """
    file_root = config[project]["file_root"]
    copy_file(master_file_path, target_file_path)  # TODO actual masterfile processing to webfile using XSLT


def check_publication_mtimes_and_publish_files(project):
    project_id = get_project_id_from_name(project)
    project_settings = config.get(project, None)
    if project_id is not None and project_settings is not None:
        file_root = project_settings.get("file_root", None)
        if file_root is not None:
            connection = db_engine.connect()

            # publication & publication_comment info
            publication_query = text("SELECT publication.id, publication.publication_collection_id, publication.original_filename, publication_comment.original_filename "
                                     "FROM publication JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                     "JOIN publication_comment ON publication.publication_comment_id=publication_comment.id "
                                     "WHERE publication_collection.project_id = :proj").bindparams(proj=project_id)

            # publication_version info
            variant_query = text("SELECT publication_version.id, publication.id, publication.publication_collection_id, publication_version.original_filename "
                                 "FROM publication_version JOIN publication ON publication_version.publication_id=publication.id "
                                 "JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                 "WHERE publication_collection.project_id = :proj").bindparams(proj=project_id)

            # publication_manuscript info
            manuscript_query = text("SELECT publication_manuscript.id, publication.id, publication.publication_collection_id, publication_manuscript.original_filename "
                                    "FROM publication_manuscript JOIN publication ON publication_manuscript.publication_id=publication.id "
                                    "JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                    "WHERE publication_collection.project_id = :proj").bindparams(proj=project_id)

            publication_info = connection.execute(publication_query).fetchall()
            variant_info = connection.execute(variant_query).fetchall()
            manuscript_info = connection.execute(manuscript_query).fetchall()

            connection.close()

            changes = []
            for row in publication_info:
                est_target_filename = "{}_{}_est.xml".format(row["publication.id"],
                                                             row["publication.publication_collection_id"])
                com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")
                est_target_file_path = os.path.join(file_root, "xml", "est", est_target_filename)
                com_target_file_path = os.path.join(file_root, "xml", "com", com_target_filename)
                est_source_file_path = os.path.join(file_root, row["publication.original_filename"])
                com_source_file_path = os.path.join(file_root, row["publication_comment.original_filename"])

                try:
                    est_target_mtime = os.path.getmtime(est_target_file_path)
                    com_target_mtime = os.path.getmtime(com_target_file_path)
                    est_source_mtime = os.path.getmtime(est_source_file_path)
                    com_source_mtime = os.path.getmtime(com_source_file_path)
                except OSError:
                    print("Error getting time_modified for target or source files for publication {}".format(row["publication.id"]))
                    print("Generating new est/com files...")
                    changes.append(est_target_file_path)
                    changes.append(com_target_file_path)
                    generate_est_and_com_files(project_name, est_source_file_path, com_source_file_path,
                                               est_target_file_path, com_target_file_path)
                else:
                    if est_target_mtime >= est_source_mtime and com_target_mtime >= com_source_mtime:
                        continue
                    else:
                        changes.append(est_target_file_path)
                        changes.append(com_target_file_path)
                        print("Reading files for publication {} are outdated, generating new est/com files...".format(row["publication.id"]))
                        generate_est_and_com_files(project_name, est_source_file_path, com_source_file_path,
                                                   est_target_file_path, com_target_file_path)

            for row in variant_info:
                target_filename = "{}_{}_var_{}.xml".format(row["publication.publication_collection_id"],
                                                            row["publication.id"],
                                                            row["publication_version.id"])
                source_filename = row["publication_version.original_filename"]
                target_file_path = os.path.join(file_root, "xml", "var", target_filename)
                source_file_path = os.path.join(file_root, source_filename)

                try:
                    target_mtime = os.path.getmtime(target_file_path)
                    source_mtime = os.path.getmtime(source_file_path)
                except OSError:
                    print("Error getting time_modified for target or source files for publication_version {}".format(row["publication_version.id"]))
                    print("Generating new file...")
                    changes.append(target_file_path)
                    generate_var_file(project_name, source_file_path, target_file_path)
                else:
                    if target_mtime >= source_mtime:
                        continue
                    else:
                        changes.append(target_file_path)
                        print("File {} is newer than source file {}, generating new file...".format(target_file_path, source_file_path))
                        generate_var_file(project_name, source_file_path, target_file_path)

            for row in manuscript_info:
                target_filename = "{}_{}_ms_{}.xml".format(row["publication.publication_collection_id"],
                                                           row["publication.id"],
                                                           row["publication_manuscript.id"])
                source_filename = row["publication_manuscript.original_filename"]
                target_file_path = os.path.join(file_root, "xml", "ms", target_filename)
                source_file_path = os.path.join(file_root, source_filename)

                try:
                    target_mtime = os.path.getmtime(target_file_path)
                    source_mtime = os.path.getmtime(source_file_path)
                except OSError:
                    print("Error getting time_modified for target or source file for publication_manuscript {}".format(row["publication_manuscript.id"]))
                    print("Generating new file...")
                    changes.append(target_file_path)
                    generate_ms_file(project_name, source_file_path, target_file_path)
                else:
                    if target_mtime >= source_mtime:
                        continue
                    else:
                        changes.append(target_file_path)
                        print("File {} is newer than source file {}, generating new file...".format(target_file_path, source_file_path))
                        generate_ms_file(project_name, source_file_path, target_file_path)

            if len(changes) > 0:
                try:
                    for change in changes:
                        run_git_command(project_name, ["add", change])
                    run_git_command(project_name, ["commit", "--author=Publisher <is@sls.fi>", "-m", "Published new web files"])
                    run_git_command(project_name, ["push"])
                except CalledProcessError:
                    print("Exception during git sync of webfile changes.")
                    print(traceback.format_exc())


if __name__ == "__main__":
    # For each project with a valid entry in the config file, check modification times for publications and publish
    for project_name in config.keys():
        check_publication_mtimes_and_publish_files(project_name)
