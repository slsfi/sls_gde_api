import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from subprocess import CalledProcessError
import traceback

from sls_api.endpoints.generics import config, db_engine, get_project_id_from_name
from sls_api.endpoints.tools_files import run_git_command
from sls_api.scripts.CTeiDocument import CTeiDocument

comment_db_engines = {project: create_engine(config[project]["comment_database"], pool_pre_ping=True) for project in config.keys()}


def get_comment_from_database(project, document_note_id):
    """
    Given the name of a project and the ID of a comment in a comment master file, returns data from the comments database with a matching documentnote.id
    Returns data as a dict-like object, can be accessed with integer index or column name (comment[0] or comment["id"])
    """
    connection = comment_db_engines[project].connect()

    comment_query = text("SELECT documentnote.id, documentnote.shortenedSelection, note.description "
                         "FROM documentnote INNER JOIN note ON documentnote.note_id = note.id "
                         "WHERE documentnote.deleted = 0 AND note.deleted = 0 AND documentnote.id = :docnote_id")
    comment_query = comment_query.bindparams(docnote_id=document_note_id)
    comment = connection.execute(comment_query).fetchone()  # should only be one, as documentnote.id should be unique
    connection.close()
    return comment


def get_all_comments_by_document_id(project, document_id):
    """
    Given the name of a project and the ID of a comments document, gets all comments from the comments database belonging to that document.
    Returns a list of dicts, each dict representing one comment.
    """
    connection = comment_db_engines[project].connect()

    comment_query = text("SELECT documentnote.id, documentnote.shortenedSelection, note.description "
                         "FROM documentnote INNER JOIN note ON documentnote.note_id = note.id "
                         "WHERE documentnote.deleted = 0 AND note.deleted = 0 AND documentnote.document_id = :doc_id")
    comment_query = comment_query.bindparams(doc_id=document_id)
    comments = connection.execute(comment_query).fetchall()
    connection.close()
    return [dict(comment) for comment in comments]


def generate_est_and_com_files(est_master_file_path, com_master_file_path, est_target_path, com_target_path):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    est_document = CTeiDocument()
    est_document.Load(est_master_file_path, bRemoveDelSpans=True)
    est_document.PostProcessMainText()
    est_document.Save(est_target_path)

    # TODO com files, check how ProcessComments works
    com_document = CTeiDocument()
    com_document.Load(com_master_file_path)
    # com_document.ProcessCommments()
    # com_document.PostProcessOtherText()
    # com_document.Save(com_target_path)


def generate_var_file(master_file_path, target_file_path):
    """
    Given a project name, and valid master and target file paths for a publication version, regenerates target file based on source file
    """
    var_document = CTeiDocument()
    var_document.Load(master_file_path)
    # TODO var files, check how ProcessVariants works
    # var_document.ProcessVariants()
    # var_document.Save(target_file_path)


def generate_ms_file(master_file_path, target_file_path):
    """
    Given a project name, and valid master and target file paths for a publication manuscript, regenerates target file based on source file
    """
    ms_document = CTeiDocument()
    ms_document.Load(master_file_path)
    ms_document.PostProcessOtherText()
    ms_document.Save(target_file_path)


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

            # Keep a list of changed files for later git commit
            changes = []
            # For each publication belonging to this project, check the modification timestamp of its master files and compare them to the generated web XML files
            for row in publication_info:
                est_target_filename = "{}_{}_est.xml".format(row["publication.id"],
                                                             row["publication.publication_collection_id"])
                com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")
                est_target_file_path = os.path.join(file_root, "xml", "est", est_target_filename)
                com_target_file_path = os.path.join(file_root, "xml", "com", com_target_filename)
                est_source_file_path = os.path.join(file_root, row["publication.original_filename"])          # original_filename should be relative to the project root
                com_source_file_path = os.path.join(file_root, row["publication_comment.original_filename"])  # original_filename should be relative to the project root

                try:
                    est_target_mtime = os.path.getmtime(est_target_file_path)
                    com_target_mtime = os.path.getmtime(com_target_file_path)
                    est_source_mtime = os.path.getmtime(est_source_file_path)
                    com_source_mtime = os.path.getmtime(com_source_file_path)
                except OSError:
                    # If there is an error, the web XML files likely don't exist or are otherwise corrupt
                    # It is then easiest to just generate new ones
                    print("Error getting time_modified for target or source files for publication {}".format(row["publication.id"]))
                    print("Generating new est/com files...")
                    changes.append(est_target_file_path)
                    changes.append(com_target_file_path)
                    generate_est_and_com_files(est_source_file_path, com_source_file_path,
                                               est_target_file_path, com_target_file_path)
                else:
                    if est_target_mtime >= est_source_mtime and com_target_mtime >= com_source_mtime:
                        # If both the est and com files are newer than the source files, just continue to the next publication
                        continue
                    else:
                        # If one or either is outdated, generate new ones
                        changes.append(est_target_file_path)
                        changes.append(com_target_file_path)
                        print("Reading files for publication {} are outdated, generating new est/com files...".format(row["publication.id"]))
                        generate_est_and_com_files(est_source_file_path, com_source_file_path,
                                                   est_target_file_path, com_target_file_path)

            # For each publication_version belonging to this project, check the modification timestamp of its master file and compare it to the generated web XML file
            for row in variant_info:
                target_filename = "{}_{}_var_{}.xml".format(row["publication.publication_collection_id"],
                                                            row["publication.id"],
                                                            row["publication_version.id"])
                source_filename = row["publication_version.original_filename"]
                target_file_path = os.path.join(file_root, "xml", "var", target_filename)
                source_file_path = os.path.join(file_root, source_filename)  # original_filename should be relative to the project root

                try:
                    target_mtime = os.path.getmtime(target_file_path)
                    source_mtime = os.path.getmtime(source_file_path)
                except OSError:
                    # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                    # It is then easiest to just generate a new one
                    print("Error getting time_modified for target or source files for publication_version {}".format(row["publication_version.id"]))
                    print("Generating new file...")
                    changes.append(target_file_path)
                    generate_var_file(source_file_path, target_file_path)
                else:
                    if target_mtime >= source_mtime:
                        # If the target var file is newer than the source, continue to the next publication_version
                        continue
                    else:
                        changes.append(target_file_path)
                        print("File {} is newer than source file {}, generating new file...".format(target_file_path, source_file_path))
                        generate_var_file(source_file_path, target_file_path)

            # For each publication_manuscript belonging to this project, check the modification timestamp of its master file and compare it to the generated web XML file
            for row in manuscript_info:
                target_filename = "{}_{}_ms_{}.xml".format(row["publication.publication_collection_id"],
                                                           row["publication.id"],
                                                           row["publication_manuscript.id"])
                source_filename = row["publication_manuscript.original_filename"]
                target_file_path = os.path.join(file_root, "xml", "ms", target_filename)
                source_file_path = os.path.join(file_root, source_filename)  # original_filename should be relative to the project root

                try:
                    target_mtime = os.path.getmtime(target_file_path)
                    source_mtime = os.path.getmtime(source_file_path)
                except OSError:
                    # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                    # It is then easiest to just generate a new one
                    print("Error getting time_modified for target or source file for publication_manuscript {}".format(row["publication_manuscript.id"]))
                    print("Generating new file...")
                    changes.append(target_file_path)
                    generate_ms_file(source_file_path, target_file_path)
                else:
                    if target_mtime >= source_mtime:
                        # If the target ms file is newer than the source, continue to the next publication_manuscript
                        continue
                    else:
                        changes.append(target_file_path)
                        print("File {} is newer than source file {}, generating new file...".format(target_file_path, source_file_path))
                        generate_ms_file(source_file_path, target_file_path)

            if len(changes) > 0:
                # If there are changes, try to commit them to git
                try:
                    for change in changes:
                        # Each changed file should be added, as there may be other activity in the git repo we don't want to commit
                        run_git_command(project_name, ["add", change])
                    # Using Publisher as the author with the is@sls.fi email as a contact point should be fine
                    run_git_command(project_name, ["commit", "--author=Publisher <is@sls.fi>", "-m", "Published new web files"])
                    run_git_command(project_name, ["push"])
                except CalledProcessError:
                    print("Exception during git sync of webfile changes.")
                    print(traceback.format_exc())


if __name__ == "__main__":
    # TODO argparse to let you publish a single project if needed - for testing and debugging
    # For each project with a valid entry in the config file, check modification times for publications and publish
    for project_name in config.keys():
        check_publication_mtimes_and_publish_files(project_name)
