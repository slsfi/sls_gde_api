import argparse
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from subprocess import CalledProcessError
import sys
from typing import Union

from sls_api.endpoints.generics import calculate_checksum, config, db_engine, get_project_id_from_name
from sls_api.endpoints.tools.files import run_git_command, update_files_in_git_repo
from sls_api.scripts.CTeiDocument import CTeiDocument

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("publisher")
logger.setLevel(logging.DEBUG)

valid_projects = [project for project in config if isinstance(config[project], dict) and config[project].get("comments_database", False)]

comment_db_engines = {project: create_engine(config[project]["comments_database"], pool_pre_ping=True) for project in valid_projects}

# comment_db_engines = {"topelius": create_engine("mysql://web_user:SecretPassword@mysql.example.com:3306/topelius_notes", pool_pre_ping=True)}

COMMENTS_XSL_PATH_IN_FILE_ROOT = "xslt/comment_html_to_tei.xsl"
COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT = "templates/comment.xml"


def get_comments_from_database(project, document_note_ids):
    if document_note_ids is None:
        return []
    """
    Given the name of a project and a list of IDs of comments in a master file, returns data from the comments database with matching documentnote.id
    Returns a list of dicts, each dict representing one comment.
    """
    connection = comment_db_engines[project].connect()

    comment_query = text("SELECT documentnote.id, documentnote.shortenedSelection, note.description \
                         FROM documentnote INNER JOIN note ON documentnote.note_id = note.id \
                         WHERE documentnote.deleted = 0 AND note.deleted = 0 AND documentnote.id IN :docnote_ids")
    comments = connection.execute(comment_query, docnote_ids=tuple(document_note_ids)).fetchall()
    connection.close()
    if len(comments) <= 0:
        return []
    return [dict(comment) for comment in comments]


def get_letter_info_from_database(letter_id):
    logger.info("Getting correspondence info for letter: {}".format(letter_id))
    if letter_id is None:
        return []
    letter = dict()
    # Get Sender
    sender = get_letter_person(letter_id, 'avsändare')
    if sender is not None:
        letter['sender'] = sender['full_name']
        letter['sender_id'] = sender['id']
    else:
        letter['sender'] = ''
        letter['sender_id'] = ''
    # Get Reciever
    reciever = get_letter_person(letter_id, 'mottagare')
    if reciever is not None:
        letter['reciever'] = reciever['full_name']
        letter['reciever_id'] = reciever['id']
    else:
        letter['reciever'] = ''
        letter['reciever_id'] = ''
    # Get Sender Location
    sender_location = get_letter_location(letter_id, 'avsändarort')
    if sender_location is not None:
        letter['sender_location'] = sender_location['name']
        letter['sender_location_id'] = sender_location['id']
    else:
        letter['sender_location'] = ''
        letter['sender_location_id'] = ''
    # Get Reciever Location
    reciever_location = get_letter_location(letter_id, 'mottagarort')
    if reciever_location is not None:
        letter['reciever_location'] = reciever_location['name']
        letter['reciever_location_id'] = reciever_location['id']
    else:
        letter['reciever_location'] = ''
        letter['reciever_location_id'] = ''
    # Get Title and Status
    title = get_letter_info(letter_id)
    if title is not None:
        letter['title'] = title['title']
        letter['title_id'] = title['id']
    else:
        letter['title'] = ''
        letter['title_id'] = ''
    return letter


def get_letter_info(letter_id):
    if letter_id is None:
        return []
    connection = db_engine.connect()
    statement = text("SELECT c.id, c.title from correspondence c \
                     where c.legacy_id = :letter_id ")
    data = connection.execute(statement, letter_id=letter_id).fetchone()
    connection.close()
    return data


def get_letter_person(letter_id, type):
    if letter_id is None:
        return []
    if type not in ['mottagare', 'avsändare']:
        return []
    connection = db_engine.connect()
    statement = text("SELECT s.id, s.full_name from correspondence c \
                     join event_connection ec on ec.correspondence_id = c.id \
                     join subject s on s.id = ec.subject_id \
                     where c.legacy_id = :letter_id and ec.type = :type ")
    data = connection.execute(statement, letter_id=letter_id, type=type).fetchone()
    connection.close()
    return data


def get_letter_location(letter_id, type):
    if letter_id is None:
        return []
    if type not in ['mottagarort', 'avsändarort']:
        return []
    connection = db_engine.connect()
    statement = text("SELECT l.id, l.name from correspondence c \
                     join event_connection ec on ec.correspondence_id = c.id \
                     join location l on l.id = ec.location_id \
                     where c.legacy_id = :letter_id and ec.type = :type ")
    data = connection.execute(statement, letter_id=letter_id, type=type).fetchone()
    connection.close()
    return data


def generate_est_and_com_files(publication_info, project, est_master_file_path, com_master_file_path, est_target_path, com_target_path, com_xsl_path=None):
    """
    Given a project name, and paths to valid EST/COM masters and targets, regenerates target files based on source files
    """
    # Generate est file for this document
    est_document = CTeiDocument()
    try:
        est_document.Load(est_master_file_path, bRemoveDelSpans=True)
        est_document.PostProcessMainText()
    except Exception as ex:
        logger.exception("Failed to handle est master file: {}".format(est_master_file_path))
        raise ex

    if publication_info is not None:
        est_document.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                 publication_info['genre'], 'est', publication_info['c_id'], publication_info['publication_group_id'])
        letterId = est_document.GetLetterId()
        if letterId is not None:
            letterData = get_letter_info_from_database(letterId)
            est_document.SetLetterTitleAndStatusAndMeta(letterData)
    try:
        est_document.Save(est_target_path)
    except Exception as ex:
        logger.exception("Failed to save est document {}".format(est_target_path))
        raise ex

    # Get all documentnote IDs from the main master file (these are the IDs of the comments for this document)
    note_ids = est_document.GetAllNoteIDs()
    # Use these note_ids to get all comments for this publication from the notes database
    comments = get_comments_from_database(project, note_ids)

    # generate comments file for this document
    com_document = CTeiDocument()

    # if com_master_file_path doesn't exist, use COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT
    if not os.path.exists(com_master_file_path):
        com_master_file_path = os.path.join(
            config[project]["file_root"], COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

    # load in com_master file
    try:
        com_document.Load(com_master_file_path)

        # if com_xsl_path is invalid or not given, try using COMMENTS_XSL_PATH_IN_FILE_ROOT
        if com_xsl_path is None or not os.path.exists(com_xsl_path):
            com_xsl_path = os.path.join(
                config[project]["file_root"], COMMENTS_XSL_PATH_IN_FILE_ROOT)

        # process comments and save
        com_document.ProcessCommments(comments, est_document, com_xsl_path)
        com_document.PostProcessOtherText()

        if publication_info is not None:
            com_document.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                     publication_info['genre'], 'com', publication_info['c_id'], publication_info['publication_group_id'])

        print(com_target_path)
        com_document.Save(com_target_path)
    except Exception as ex:
        logger.exception("Failed to handle com master file: {}".format(com_master_file_path))
        raise ex


def process_var_documents_and_generate_files(main_var_doc, main_var_path, var_docs, var_paths, publication_info):
    """
    Process generated CTeiDocument objects - comparing each var_doc in var_docs to the main_var_doc and saving target files
    """
    # First, compare the main variant against all other variants
    main_var_doc.ProcessVariants(var_docs)
    if publication_info is not None:
        main_var_doc.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                 publication_info['genre'], 'com', publication_info['c_id'], publication_info['publication_group_id'])
    # Then save main variant web XML file
    main_var_doc.Save(main_var_path)
    # lastly, save all other variant web XML files
    for var_doc, var_path in zip(var_docs, var_paths):
        var_doc.Save(var_path)


def generate_ms_file(master_file_path, target_file_path, publication_info):
    """
    Given a project name, and valid master and target file paths for a publication manuscript, regenerates target file based on source file
    """
    try:
        ms_document = CTeiDocument()
        ms_document.Load(master_file_path)
        ms_document.PostProcessOtherText()
    except Exception as ex:
        logger.exception("Failed to handle manuscript file: {}".format(master_file_path))
        raise ex

    if publication_info is not None:
        ms_document.SetMetadata(publication_info['original_publication_date'], publication_info['p_id'], publication_info['name'],
                                publication_info['genre'], 'com', publication_info['c_id'], publication_info['publication_group_id'])
    ms_document.Save(target_file_path)


def check_publication_mtimes_and_publish_files(project: str, publication_ids: Union[tuple, None], git_author: str, no_git=False, force_publish=False, is_multilingual=False):
    update_success, result_str = update_files_in_git_repo(project)
    if not update_success:
        logger.error("Git update failed! Reason: {}".format(result_str))
        return False
    project_id = get_project_id_from_name(project)
    project_settings = config.get(project, None)

    # if publication_ids is a tuple of ints, we're (re)publishing a certain publication(s)
    # explicitly set force_publish in this instance, so we force-generate files for publishing (this overrides mtime checks)
    if isinstance(publication_ids, tuple):
        force_publish = True

    if project_id is not None and project_settings is not None:
        file_root = project_settings.get("file_root", None)
        if file_root is not None:
            # open DB connection for publication, comment, and manuscript data fetch
            connection = db_engine.connect()

            # publication info
            publication_query = "SELECT \
                                p.id as p_id, \
                                p.publication_collection_id as c_id, \
                                pcol.id as c_id, \
                                p.original_filename as original_filename, \
                                p.original_publication_date as original_publication_date, \
                                p.genre as genre, \
                                p.publication_group_id as publication_group_id, \
                                p.publication_comment_id as publication_comment_id, \
                                p.name as name \
                                FROM publication p \
                                JOIN publication_collection pcol ON p.publication_collection_id=pcol.id \
                                WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 "

            if is_multilingual:
                # publication info
                publication_query = "SELECT \
                                    p.id as p_id, \
                                    p.publication_collection_id as c_id, \
                                    pcol.id as c_id, \
                                    tr.text as original_filename, \
                                    p.original_publication_date as original_publication_date, \
                                    p.genre as genre, \
                                    p.publication_group_id as publication_group_id, \
                                    p.publication_comment_id as publication_comment_id, \
                                    p.name as name, \
                                    tr.language as language \
                                    FROM publication p \
                                    JOIN publication_collection pcol ON p.publication_collection_id=pcol.id \
                                    JOIN translation_text tr ON p.translation_id = tr.translation_id and tr.field_name='original_filename' \
                                    WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 "

            # publication_comment info, relating to "general comments" file for each publication
            comment_query = "SELECT \
                            p.id as p_id, \
                            p.publication_collection_id as c_id, \
                            pc.original_filename as original_filename, \
                            p.original_publication_date as original_publication_date, \
                            p.genre as genre, \
                            p.publication_group_id as publication_group_id, \
                            p.publication_comment_id as publication_comment_id, \
                            p.name as name \
                            FROM publication p \
                            JOIN publication_collection pcol ON p.publication_collection_id = pcol.id \
                            JOIN publication_comment pc ON p.publication_comment_id = pc.id \
                            WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 AND pc.deleted != 1 "

            # publication_manuscript info
            manuscript_query = "SELECT \
                                pm.id as m_id, \
                                p.id as p_id, \
                                p.publication_collection_id as c_id, \
                                pcol.id as c_id, \
                                pm.original_filename as original_filename, \
                                p.original_publication_date as original_publication_date, \
                                p.genre as genre, \
                                p.publication_group_id as publication_group_id, \
                                p.publication_comment_id as publication_comment_id, \
                                p.name as name \
                                FROM publication_manuscript pm \
                                JOIN publication p ON pm.publication_id = p.id \
                                JOIN publication_collection pcol ON p.publication_collection_id = pcol.id \
                                WHERE pcol.project_id = :proj AND p.deleted != 1 AND pcol.deleted != 1 AND pm.deleted != 1 "

            if force_publish and isinstance(publication_ids, tuple):
                # append publication.id checks if this is a forced (re)publication of certain publication(s)
                publication_query += " AND p.id IN :p_ids"
                publication_query = text(publication_query).bindparams(proj=project_id, p_ids=publication_ids)

                comment_query += " AND p.id IN :p_ids"
                comment_query = text(comment_query).bindparams(proj=project_id, p_ids=publication_ids)

                manuscript_query += " AND p.id IN :p_ids"
                manuscript_query = text(manuscript_query).bindparams(proj=project_id, p_ids=publication_ids)
            else:
                publication_query = text(publication_query).bindparams(proj=project_id)
                comment_query = text(comment_query).bindparams(proj=project_id)
                manuscript_query = text(manuscript_query).bindparams(proj=project_id)

            publication_info = connection.execute(publication_query).fetchall()
            manuscript_info = connection.execute(manuscript_query).fetchall()

            # comment_filenames can just be a dict of publication.id to publication_comment.original_filename
            comment_filenames = dict()
            for row in connection.execute(comment_query):
                comment_filenames[row["p_id"]] = row["original_filename"]

            # close DB connection for now, it won't be needed for a while
            connection.close()

            # Keep a list of changed files for later git commit
            changes = set()
            # logger.debug("Publication query resulting rows: {}".format(publication_info[0].keys()))  TODO: fix IndexError if publication_info has no rows
            # For each publication belonging to this project, check the modification timestamp of its master files and compare them to the generated web XML files
            for row in publication_info:
                publication_id = row["p_id"]
                collection_id = row["c_id"]
                if not row["original_filename"]:
                    logger.info("Source file not set for publication {}".format(publication_id))
                    continue
                est_target_filename = "{}_{}_est.xml".format(collection_id, publication_id)
                com_target_filename = est_target_filename.replace("_est.xml", "_com.xml")

                if is_multilingual:
                    language = row["language"]
                    est_target_filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)

                est_target_file_path = os.path.join(file_root, "xml", "est", est_target_filename)
                com_target_file_path = os.path.join(file_root, "xml", "com", com_target_filename)
                # original_filename should be relative to the project root
                est_source_file_path = os.path.join(file_root, row["original_filename"])

                # default to template comment file if no entry in publication_comment pointing to a comments file for this publication
                comment_file = comment_filenames.get(publication_id, COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT)

                if comment_file is None:
                    logger.info("Comment file not set for publication {}, using template instead.".format(publication_id))
                    comment_file = COMMENTS_TEMPLATE_PATH_IN_FILE_ROOT

                com_source_file_path = os.path.join(file_root, comment_file)

                if os.path.isdir(est_source_file_path):
                    logger.warning("Source file {} for publication {} is a directory!".format(est_source_file_path, publication_id))
                    continue
                if os.path.isdir(com_source_file_path):
                    logger.warning("Source file {} for publication {} comment is a directory!".format(com_source_file_path, publication_id))
                    continue
                if not os.path.exists(est_source_file_path):
                    logger.warning("Source file {} for publication {} does not exist!".format(est_source_file_path, publication_id))
                    continue
                if not os.path.exists(com_source_file_path):
                    logger.warning("Source file {} for publication {} does not exist!".format(com_source_file_path, publication_id))
                    continue

                if force_publish:
                    # during force_publish, just generate
                    logger.info("Generating new est/com files for publication {}...".format(publication_id))
                    try:
                        # calculate md5sum for existing files
                        md5sums = []
                        if os.path.exists(est_target_file_path):
                            md5sums.append(calculate_checksum(est_target_file_path))
                        else:
                            md5sums.append("SKIP")
                        if os.path.exists(com_target_file_path):
                            md5sums.append(calculate_checksum(com_target_file_path))
                        else:
                            md5sums.append("SKIP")
                        generate_est_and_com_files(row, project, est_source_file_path, com_source_file_path,
                                                   est_target_file_path, com_target_file_path)
                    except Exception as ex:
                        logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                        raise ex
                        continue
                    else:
                        # only add files to change set if they actually changed
                        if md5sums[0] == "SKIP" or md5sums[0] != calculate_checksum(est_target_file_path):
                            changes.add(est_target_file_path)
                        if md5sums[1] == "SKIP" or md5sums[1] != calculate_checksum(com_target_file_path):
                            changes.add(com_target_file_path)
                else:
                    # otherwise, check if this publication's files need to be re-generated
                    try:
                        est_target_mtime = os.path.getmtime(est_target_file_path)
                        com_target_mtime = os.path.getmtime(com_target_file_path)
                        est_source_mtime = os.path.getmtime(est_source_file_path)
                        com_source_mtime = os.path.getmtime(com_source_file_path)
                    except OSError:
                        # If there is an error, the web XML files likely don't exist or are otherwise corrupt
                        # It is then easiest to just generate new ones
                        logger.warning("Error getting time_modified for target or source files for publication {}".format(publication_id))
                        logger.info("Generating new est/com files for publication {}...".format(publication_id))
                        try:
                            # calculate md5sum for existing files
                            md5sums = []
                            if os.path.exists(est_target_file_path):
                                md5sums.append(calculate_checksum(est_target_file_path))
                            else:
                                md5sums.append("SKIP")
                            if os.path.exists(com_target_file_path):
                                md5sums.append(calculate_checksum(com_target_file_path))
                            else:
                                md5sums.append("SKIP")
                            generate_est_and_com_files(row, project, est_source_file_path, com_source_file_path,
                                                       est_target_file_path, com_target_file_path)
                        except Exception:
                            logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                            continue
                        else:
                            # only add files to change set if they actually changed
                            if md5sums[0] == "SKIP" or md5sums[0] != calculate_checksum(est_target_file_path):
                                changes.add(est_target_file_path)
                            if md5sums[1] == "SKIP" or md5sums[1] != calculate_checksum(com_target_file_path):
                                changes.add(com_target_file_path)
                    else:
                        if est_target_mtime >= est_source_mtime and com_target_mtime >= com_source_mtime:
                            # If both the est and com files are newer than the source files, just continue to the next publication
                            continue
                        else:
                            # If one or either is outdated, generate new ones
                            logger.info("Reading files for publication {} are outdated, generating new est/com files...".format(publication_id))
                            try:
                                # calculate md5sum for existing files
                                md5sums = []
                                if os.path.exists(est_target_file_path):
                                    md5sums.append(calculate_checksum(est_target_file_path))
                                else:
                                    md5sums.append("SKIP")
                                if os.path.exists(com_target_file_path):
                                    md5sums.append(calculate_checksum(com_target_file_path))
                                else:
                                    md5sums.append("SKIP")
                                generate_est_and_com_files(row, project, est_source_file_path, com_source_file_path,
                                                           est_target_file_path, com_target_file_path)
                            except Exception:
                                logger.exception("Failed to generate est/com files for publication {}!".format(publication_id))
                                continue
                            else:
                                # only add files to change set if they actually changed
                                if md5sums[0] == "SKIP" or md5sums[0] != calculate_checksum(est_target_file_path):
                                    changes.add(est_target_file_path)
                                if md5sums[1] == "SKIP" or md5sums[1] != calculate_checksum(com_target_file_path):
                                    changes.add(com_target_file_path)

                # Process all variants belonging to this publication
                # publication_version with type=1 is the "main" variant, the others should have type=2 and be versions of that main variant
                variant_query = text("SELECT id, original_filename "
                                     "FROM publication_version "
                                     "WHERE publication_version.publication_id = :pub_id AND publication_version.type = :vers_type AND publication_version.deleted != 1")

                # open new DB connection for variant data fetch
                connection = db_engine.connect()

                # fetch info for "main" variant
                main_variant_query = variant_query.bindparams(pub_id=publication_id, vers_type=1)
                # should only be one main variant per publication?
                main_variant_info = connection.execute(main_variant_query).fetchone()
                if main_variant_info is None:
                    logger.warning("No main variant found for publication {}!".format(publication_id))
                else:
                    logger.debug("Main variant query resulting rows: {}".format(main_variant_info.keys()))

                    # fetch info for all "other" variants
                    variants_query = variant_query.bindparams(pub_id=publication_id, vers_type=2)
                    variants_info = connection.execute(variants_query).fetchall()
                    # logger.debug("Variants query resulting rows: {}".format(variants_info[0].keys())) TODO: fix IndexError if variants_info has no rows

                    # close DB connection, as it's no longer needed
                    connection.close()

                    # compile info and generate files if needed
                    if main_variant_info["original_filename"] is None:
                        continue

                    main_variant_source = os.path.join(file_root, main_variant_info["original_filename"])

                    if not main_variant_source:
                        logger.warning("Source file for main variant {} is not set.".format(main_variant_info["id"]))
                        continue

                    if os.path.isdir(main_variant_source):
                        logger.error("Source file {} for main variant {} (type=1) is a directory!".format(main_variant_source, main_variant_info["id"]))
                        continue

                    if not os.path.exists(main_variant_source):
                        logger.error("Source file {} for main variant {} (type=1) does not exist!".format(main_variant_source, main_variant_info["id"]))
                        continue

                    target_filename = "{}_{}_var_{}.xml".format(collection_id,
                                                                publication_id,
                                                                main_variant_info["id"])

                    # If any variants have changed, we need a CTeiDocument for the main variant to ProcessVariants() with
                    main_variant_target = os.path.join(file_root, "xml", "var", target_filename)
                    # check current md5sum for main variant file
                    if os.path.exists(main_variant_target):
                        main_variant_md5 = calculate_checksum(main_variant_target)
                    else:
                        main_variant_md5 = "SKIP"
                    main_variant_doc = CTeiDocument()
                    main_variant_doc.Load(main_variant_source)

                    # For each "other" variant, create a new CTeiDocument if needed, but if main_variant_updated is True, just make a new for all
                    variant_docs = []
                    variant_paths = []
                    for variant in variants_info:
                        target_filename = "{}_{}_var_{}.xml".format(collection_id,
                                                                    publication_id,
                                                                    variant["id"])
                        if variant["original_filename"] is None:
                            continue

                        source_filename = variant["original_filename"]
                        if not source_filename:
                            logger.error("Source file for variant {} is not set.".format(variant["id"]))
                            continue
                        target_file_path = os.path.join(file_root, "xml", "var", target_filename)
                        # original_filename should be relative to the project root
                        source_file_path = os.path.join(file_root, source_filename)

                        if os.path.isdir(source_file_path):
                            logger.error("Source file {} for variant {} is a directory!".format(source_file_path, variant["id"]))
                            continue
                        if not os.path.exists(source_file_path):
                            logger.error("Source file {} for variant {} does not exist!".format(source_file_path, variant["id"]))
                            continue

                        # in a force_publish, just load all variants for generation/processing
                        if force_publish:
                            logger.info("Generating new var file for publication_version {}...".format(variant["id"]))
                            variant_doc = CTeiDocument()
                            variant_doc.Load(source_file_path)
                            variant_docs.append(variant_doc)
                            variant_paths.append(target_file_path)
                        # otherwise, check which ones need to be updated and load only those
                        else:
                            try:
                                target_mtime = os.path.getmtime(target_file_path)
                                source_mtime = os.path.getmtime(source_file_path)
                            except OSError:
                                # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                                # It is then easiest to just generate a new one
                                logger.warning("Error getting time_modified for target or source files for publication_version {}".format(variant["id"]))
                                logger.info("Generating new file...")
                                variant_doc = CTeiDocument()
                                variant_doc.Load(source_file_path)
                                variant_docs.append(variant_doc)
                                variant_paths.append(target_file_path)
                            else:
                                if target_mtime < source_mtime:
                                    logger.info("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
                                    variant_doc = CTeiDocument()
                                    variant_doc.Load(source_file_path)
                                    variant_docs.append(variant_doc)
                                    variant_paths.append(target_file_path)
                                else:
                                    # If no changes, don't generate CTeiDocument and don't make a new web XML file
                                    continue
                    # check current md5sum for variant files
                    variant_md5_sums = {}
                    for path in variant_paths:
                        if not os.path.exists(path):
                            variant_md5_sums[path] = "SKIP"
                        else:
                            variant_md5_sums[path] = calculate_checksum(path)
                    # lastly, actually process all generated CTeiDocument objects and create web XML files
                    process_var_documents_and_generate_files(main_variant_doc, main_variant_target, variant_docs, variant_paths, row)

                    # only add main variant file to change set if file actually changed
                    if main_variant_md5 == "SKIP" or main_variant_md5 != calculate_checksum(main_variant_target):
                        changes.add(main_variant_target)
                    # only add variant files to change set if their file actually changed
                    for path, md5sum in variant_md5_sums.items():
                        if md5sum == "SKIP" or md5sum != calculate_checksum(path):
                            changes.add(path)

            # For each publication_manuscript belonging to this project, check the modification timestamp of its master file and compare it to the generated web XML file
            # logger.debug("Manuscript query resulting rows: {}".format(manuscript_info[0].keys())) TODO: fix IndexError if manuscript_info has no rows
            for row in manuscript_info:
                collection_id = row["c_id"]
                publication_id = row["p_id"]
                manuscript_id = row["m_id"]
                target_filename = "{}_{}_ms_{}.xml".format(collection_id,
                                                           publication_id,
                                                           manuscript_id)
                if row["original_filename"] is None:
                    continue

                source_filename = row["original_filename"]
                if not source_filename:
                    logger.info("Source file not set for manuscript {}".format(manuscript_id))
                    continue

                target_file_path = os.path.join(file_root, "xml", "ms", target_filename)
                # original_filename should be relative to the project root
                source_file_path = os.path.join(file_root, source_filename)

                if os.path.isdir(source_file_path):
                    logger.warning("Source file {} for manuscript {} is a directory!".format(source_file_path, manuscript_id))
                    continue

                if not os.path.exists(source_file_path):
                    logger.warning("Source file {} for manuscript {} does not exist!".format(source_file_path, manuscript_id))
                    continue

                # in a force_publish, just generate all ms files
                if force_publish:
                    logger.info("Generating new ms file for publication_manuscript {}".format(manuscript_id))
                    try:
                        # calculate md5sum for existing file
                        if os.path.exists(target_file_path):
                            md5sum = calculate_checksum(target_file_path)
                        else:
                            md5sum = "SKIP"
                        generate_ms_file(source_file_path, target_file_path, row)
                    except Exception:
                        continue
                    else:
                        # only add to changes if file has actually changed
                        if md5sum == "SKIP" or md5sum != calculate_checksum(target_file_path):
                            changes.add(target_file_path)
                # otherwise, check if this file needs generating
                else:
                    try:
                        target_mtime = os.path.getmtime(target_file_path)
                        source_mtime = os.path.getmtime(source_file_path)
                    except OSError:
                        # If there is an error, the web XML file likely doesn't exist or is otherwise corrupt
                        # It is then easiest to just generate a new one
                        logger.warning("Error getting time_modified for target or source file for publication_manuscript {}".format(manuscript_id))
                        logger.info("Generating new file...")
                        try:
                            # calculate md5sum for existing file
                            if os.path.exists(target_file_path):
                                md5sum = calculate_checksum(target_file_path)
                            else:
                                md5sum = "SKIP"
                            generate_ms_file(source_file_path, target_file_path, row)
                        except Exception:
                            continue
                        else:
                            # only add to changes if file has actually changed
                            if md5sum == "SKIP" or md5sum != calculate_checksum(target_file_path):
                                changes.add(target_file_path)
                    else:
                        if target_mtime >= source_mtime:
                            # If the target ms file is newer than the source, continue to the next publication_manuscript
                            continue
                        else:
                            logger.info("File {} is older than source file {}, generating new file...".format(target_file_path, source_file_path))
                            try:
                                # calculate md5sum for existing file
                                if os.path.exists(target_file_path):
                                    md5sum = calculate_checksum(target_file_path)
                                else:
                                    md5sum = "SKIP"
                                generate_ms_file(source_file_path, target_file_path, row)
                            except Exception:
                                continue
                            else:
                                # only add to changes if file has actually changed
                                if md5sum == "SKIP" or md5sum != calculate_checksum(target_file_path):
                                    changes.add(target_file_path)

            logger.debug("Changes made in publication script run: {}".format([c for c in changes]))
            if len(changes) > 0 and not no_git:
                outputs = []
                # If there are changes, try to commit them to git
                try:
                    for change in changes:
                        # Each changed file should be added, as there may be other activity in the git repo we don't want to commit
                        outputs.append(run_git_command(project, ["add", change]))
                    outputs.append(run_git_command(project, ["commit", "--author", git_author, "-m", "Published new web files"]))
                    outputs.append(run_git_command(project, ["push"]))
                except CalledProcessError as e:
                    logger.exception("Exception during git sync of webfile changes.")
                    logger.exception("Exception on process, rc=", e.returncode, "output=", e.output)
                    logger.debug("Git outputs: \n")
                    logger.debug(outputs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publishing script to publish changes to EST/COM/VAR/MS files for GDE project")
    parser.add_argument("project", help="Which project to publish, either a project name from --list_projects or 'all' for all valid projects")
    parser.add_argument("-i", "--publication_ids", type=int, nargs="*",
                        help="Force re-publication of specific publications (tries to publish all files, est/com/var/ms)")
    parser.add_argument("--all_ids", action="store_true",
                        help="Force re-publication of all publications (tries to publish all files, est/com/var/ms)")
    parser.add_argument("-l", "--list_projects", action="store_true",
                        help="Print a listing of available projects with seemingly valid configuration and exit")
    parser.add_argument("--git_author", type=str, help="Author used for git commits (Default 'Publisher <is@sls.fi>')", default="Publisher <is@sls.fi>")
    parser.add_argument("--no_git", action="store_true", help="Don't run git commands as part of publishing.")
    parser.add_argument("--is_multilingual", action="store_true", help="The publication is multilingual and original_filename is found in translation_text")

    args = parser.parse_args()

    if args.list_projects:
        logger.info(f"Projects with seemingly valid configuration: {', '.join(valid_projects)}")
        sys.exit(0)
    else:
        if args.publication_ids is None:
            ids = None
        elif len(args.publication_ids) == 0:
            ids = None
        else:
            # use a tuple rather than a list, to make SQLAlchemy happier more easily
            ids = tuple(args.publication_ids)
        if str(args.project).lower() == "all":
            for p in valid_projects:
                check_publication_mtimes_and_publish_files(p, ids, git_author=args.git_author,
                                                           no_git=args.no_git, force_publish=args.all_ids)
        else:
            if args.project in valid_projects:
                check_publication_mtimes_and_publish_files(args.project, ids, git_author=args.git_author,
                                                           no_git=args.no_git, force_publish=args.all_ids, is_multilingual=args.is_multilingual)
            else:
                logger.error(f"{args.project} is not in the API configuration or lacks 'comments_database' setting, aborting...")
                sys.exit(1)
