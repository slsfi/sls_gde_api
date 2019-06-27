import argparse
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from subprocess import CalledProcessError
import sys
import traceback

from sls_api.endpoints.generics import config, db_engine, get_project_id_from_name
from sls_api.endpoints.tools_files import run_git_command, update_files_in_git_repo
from sls_api.scripts.CTeiDocument import CTeiDocument

valid_projects = [project for project in config if isinstance(config[project], dict) and config[project].get("comment_database", False)]

comment_db_engines = {project: create_engine(config[project]["comment_database"], pool_pre_ping=True) for project in valid_projects}

# comment_db_engines = {"topelius": create_engine("mysql://web_user:SecretPassword@mysql.example.com:3306/topelius_notes", pool_pre_ping=True)}

COMMENTS_XSL_PATH_IN_FILE_ROOT = "xslt/comment_html_to_tei.xsl"
COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT = "templates/comment.xml"


def get_comments_from_database(project, document_note_ids):
    """
    Given the name of a project and a list of IDs of comments in a master file, returns data from the comments database with matching documentnote.id
    Returns a list of dicts, each dict representing one comment.
    """
    connection = comment_db_engines[project].connect()

    comment_query = text("SELECT documentnote.id, documentnote.shortenedSelection, note.description "
                         "FROM documentnote INNER JOIN note ON documentnote.note_id = note.id "
                         "WHERE documentnote.deleted = 0 AND note.deleted = 0 AND documentnote.id IN :docnote_ids")
    comments = connection.execute(comment_query, docnote_ids=tuple(document_note_ids)).fetchall()
    connection.close()
    return [dict(comment) for comment in comments]


def generate_est_and_com_files(project, est_master_file_path, com_master_file_path, est_target_path, com_target_path, com_xsl_path=None):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    # Generate est file for this document
    est_document = CTeiDocument()
    est_document.Load(est_master_file_path, bRemoveDelSpans=True)
    est_document.PostProcessMainText()
    est_document.Save(est_target_path)

    # Get all documentnote IDs from the main master file (these are the IDs of the comments for this document)
    note_ids = est_document.GetAllNoteIDs()
    # Use these note_ids to get all comments for this publication from the notes database
    comments = get_comments_from_database(project, note_ids)

    # generate comments file for this document
    com_document = CTeiDocument()

    # if com_master_file_path doesn't exist, use COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT
    if not os.path.exists(com_master_file_path):
        com_master_file_path = os.path.join(config[project]["file_root"], COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

    # load in com_master file
    com_document.Load(com_master_file_path)

    # if com_xsl_path is invalid or not given, try using COMMENTS_XSL_PATH_IN_FILE_ROOT
    if com_xsl_path is None or not os.path.exists(com_xsl_path):
        com_xsl_path = os.path.join(config[project]["file_root"], COMMENTS_XSL_PATH_IN_FILE_ROOT)

    # process comments and save
    com_document.ProcessCommments(comments, est_document, com_xsl_path)
    com_document.PostProcessOtherText()
    com_document.Save(com_target_path)


def process_var_documents_and_generate_files(main_var_doc, main_var_path, var_docs, var_paths):
    """
    Process generated CTeiDocument objects - comparing each var_doc in var_docs to the main_var_doc and saving target files
    """
    # First, compare the main variant against all other variants
    main_var_doc.ProcessVariants(var_docs)
    # Then save main variant web XML file
    main_var_doc.Save(main_var_path)
    # lastly, save all other variant web XML files
    for var_doc, var_path in zip(var_docs, var_paths):
        var_doc.Save(var_path)


def generate_ms_file(master_file_path, target_file_path):
    """
    Given a project name, and valid master and target file paths for a publication manuscript, regenerates target file based on source file
    """
    ms_document = CTeiDocument()
    ms_document.Load(master_file_path)
    ms_document.PostProcessOtherText()
    ms_document.Save(target_file_path)


def check_publication_mtimes_and_publish_files(project):
    update_success, _ = update_files_in_git_repo(project)
    if not update_success:
        print("Git update failed!")
        return False
    project_id = get_project_id_from_name(project)
    project_settings = config.get(project, None)
    if project_id is not None and project_settings is not None:
        file_root = project_settings.get("file_root", None)
        if file_root is not None:
            # open DB connection for publication, comment, and manuscript data fetch
            connection = db_engine.connect()

            # publication & publication_comment info
            publication_query = text("SELECT publication.id, publication.publication_collection_id, publication.original_filename, publication_comment.original_filename "
                                     "FROM publication JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                     "JOIN publication_comment ON publication.publication_comment_id=publication_comment.id "
                                     "WHERE publication_collection.project_id = :proj").bindparams(proj=project_id)

            # publication_manuscript info
            manuscript_query = text("SELECT publication_manuscript.id, publication.id, publication.publication_collection_id, publication_manuscript.original_filename "
                                    "FROM publication_manuscript JOIN publication ON publication_manuscript.publication_id=publication.id "
                                    "JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                    "WHERE publication_collection.project_id = :proj").bindparams(proj=project_id)

            publication_info = connection.execute(publication_query).fetchall()
            manuscript_info = connection.execute(manuscript_query).fetchall()

            # close DB connection for now, it won't be needed for a while
            connection.close()

            # Keep a list of changed files for later git commit
            changes = []
            # For each publication belonging to this project, check the modification timestamp of its master files and compare them to the generated web XML files
            for row in publication_info:
                est_target_filename = "{}_{}_est.xml".format(row["publication.publication_collection_id"],
                                                             row["publication.id"])
                com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")
                est_target_file_path = os.path.join(file_root, "xml", "est", est_target_filename)
                com_target_file_path = os.path.join(file_root, "xml", "com", com_target_filename)
                est_source_file_path = os.path.join(file_root, row["publication.original_filename"])          # original_filename should be relative to the project root
                com_source_file_path = os.path.join(file_root, row["publication_comment.original_filename"])  # original_filename should be relative to the project root

                if not os.path.exists(est_source_file_path) or not os.path.exists(com_source_file_path):
                    print("Source file for est or com file for publication {} does not exist!".format(row["publication.id"]))
                    continue
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
                    generate_est_and_com_files(project_name, est_source_file_path, com_source_file_path,
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
                        generate_est_and_com_files(project_name, est_source_file_path, com_source_file_path,
                                                   est_target_file_path, com_target_file_path)

                # Process all variants belonging to this publication
                # publication_version with type=1 is the "main" variant, the others should have type=2 and be versions of that main variant
                publication_id = row["publication.id"]
                variant_query = text("SELECT publication_version.id, publication.id, publication.publication_collection_id, publication_version.original_filename "
                                     "FROM publication_version JOIN publication ON publication_version.publication_id=publication.id "
                                     "JOIN publication_collection ON publication.publication_collection_id=publication_collection.id "
                                     "WHERE publication_collection.project_id = :proj AND publication.id = :pub_id AND publication_version.type = :vers_type")

                # open new DB connection for variant data fetch
                connection = db_engine.connect()

                # fetch info for "main" variant
                main_variant_query = variant_query.bindparams(proj=project_id, pub_id=publication_id, vers_type=1)
                main_variant_info = connection.execute(main_variant_query).fetchone()   # should only be one main variant per publication?

                # fetch info for all "other" variants
                variants_query = variant_query.bindparams(proj=project_id, pub_id=publication_id, vers_type=2)
                variants_info = connection.execute(variants_query).fetchall()

                # close DB connection, as it's no longer needed
                connection.close()

                # compile info and generate files if needed
                main_variant_source = os.path.join(file_root, main_variant_info["publication_version.original_filename"])

                if not os.path.exists(main_variant_source):
                    print("Source file for main variant {} (type=1) does not exist!".format(main_variant_info["publication_version.id"]))
                    continue

                target_filename = "{}_{}_var_{}.xml".format(row["publication.publication_collection_id"],
                                                            row["publication.id"],
                                                            row["publication_version.id"])

                # If any variants have changed, we need a CTeiDocument for the main variant to ProcessVariants() with
                main_variant_target = os.path.join(file_root, "xml", "var", target_filename)
                main_variant_doc = CTeiDocument()
                main_variant_doc.Load(main_variant_source)

                # For each "other" variant, create a new CTeiDocument if needed, but if main_variant_updated is True, just make a new for all
                variant_docs = []
                variant_paths = []
                for variant in variants_info:
                    target_filename = "{}_{}_var_{}.xml".format(variant["publication.publication_collection_id"],
                                                                variant["publication.id"],
                                                                row["publication_version.id"])
                    source_filename = variant["publication_version.original_filename"]
                    target_file_path = os.path.join(file_root, "xml", "var", target_filename)
                    source_file_path = os.path.join(file_root, source_filename)  # original_filename should be relative to the project root

                    if not os.path.exists(source_file_path):
                        print("Source file for variant {} does not exist!".format(variant["publication_version.id"]))
                        continue

                    try:
                        target_mtime = os.path.getmtime(target_file_path)
                        source_mtime = os.path.getmtime(source_file_path)
                    except OSError:
                        # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                        # It is then easiest to just generate a new one
                        print("Error getting time_modified for target or source files for publication_version {}".format(variant["publication_version.id"]))
                        print("Generating new file...")
                        changes.append(target_file_path)
                        variant_doc = CTeiDocument()
                        variant_doc.Load(source_file_path)
                        variant_docs.append(variant_doc)
                        variant_paths.append(target_file_path)
                    else:
                        if target_mtime < source_mtime:
                            print("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
                            changes.append(target_file_path)
                            variant_doc = CTeiDocument()
                            variant_doc.Load(source_file_path)
                            variant_docs.append(variant_doc)
                            variant_paths.append(target_file_path)
                        else:
                            # If no changes, don't generate CTeiDocument and don't make a new web XML file
                            continue
                # lastly, actually process all generated CTeiDocument objects and create web XML files
                process_var_documents_and_generate_files(main_variant_doc, main_variant_target, variant_docs, variant_paths)

            # For each publication_manuscript belonging to this project, check the modification timestamp of its master file and compare it to the generated web XML file
            for row in manuscript_info:
                target_filename = "{}_{}_ms_{}.xml".format(row["publication.publication_collection_id"],
                                                           row["publication.id"],
                                                           row["publication_manuscript.id"])
                source_filename = row["publication_manuscript.original_filename"]
                target_file_path = os.path.join(file_root, "xml", "ms", target_filename)
                source_file_path = os.path.join(file_root, source_filename)  # original_filename should be relative to the project root

                if not os.path.exists(source_file_path):
                    print("Source file for manuscript {} does not exist!".format(row["publication_manuscript.id"]))
                    continue

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
                        print("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
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
    parser = argparse.ArgumentParser(description="Publishing script to publish changes to EST/COM/VAR/MS files for GDE project.")
    parser.add_argument("-p", "--project", help="Optional project name specification to only process the named project, rather than all projects.")

    args = parser.parse_args()

    if args.project is None:
        # For each project with a valid entry in the config file, check modification times for publications and publish
        for project_name in config.keys():
            check_publication_mtimes_and_publish_files(project_name)
    else:
        if args.project in config:
            check_publication_mtimes_and_publish_files(args.project)
        else:
            print("{} is not in the API configuration, aborting...".format(args.project))
            sys.exit(1)
