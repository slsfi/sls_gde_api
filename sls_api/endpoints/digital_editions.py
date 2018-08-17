import calendar
from collections import OrderedDict
from flask import abort, Blueprint, request, Response, safe_join
from flask.json import jsonify
import io
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import os
import sqlalchemy.sql
import time
import re
import glob

from sls_api.endpoints.generics import config, db_engine, select_all_from_table

digital_edition = Blueprint('digital_edition', __name__)

logger = logging.getLogger("sls_api.digital_edition")

file_handler = TimedRotatingFileHandler(filename=config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)


@digital_edition.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response


@digital_edition.route("/projects/")
def get_projects():
    """
    List all GDE projects
    """
    return select_all_from_table("project")


@digital_edition.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    logger.info("Getting static content from /{}/html/{}".format(project, filename))
    file_path = safe_join(config[project]["file_root"], "html", "{}.html".format(filename))
    if os.path.exists(file_path):
        with io.open(file_path, encoding="UTF-8") as html_file:
            contents = html_file.read()
        data = {
            "filename": filename,
            "content": contents
        }
        return jsonify(data), 200
    else:
        abort(404)


@digital_edition.route("/<project>/md/<fileid>")
def get_md_contents_as_json(project, fileid):
    # TODO safer handling of paths, glob.iglob is not secure with arbitrary user input to fileid

    path = "*/".join(fileid.split("-")) + "*"

    file_path_query = safe_join(config[project]["file_root"], "md", path)

    try:
        file_path = [f for f in glob.iglob(file_path_query)][0]
        print(file_path)
        if os.path.exists(file_path):
            with io.open(file_path, encoding="UTF-8") as md_file:
                contents = md_file.read()
            data = {
                "fileid": fileid,
                "content": contents
            }
            return jsonify(data), 200
        else:
            abort(404)
    except Exception:
        print(file_path_query)
        abort(404)


@digital_edition.route("/<project>/static-pages-toc/<language>")
def get_static_pages_as_json(project, language):
    logger.info("Getting static content from /{}/static-pages-toc/{}".format(project, language))
    folder_path = safe_join(config[project]["file_root"], "md", language)

    if os.path.exists(folder_path):
        data = path_hierarchy(folder_path, language)
        return jsonify(data), 200
    else:
        abort(404)


@digital_edition.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publicationManuscript WHERE publication_id=:pub_id")
    statement = sql.bindparams(pub_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)

@digital_edition.route("/<project>/text/<text_type>/<text_id>")
def get_text_by_type(project, text_type, text_id):
    logger.info("Getting text by type /{}/text/{}/{}".format(project, text_type, text_id))

    text_table = ''
    if text_type == 'manuscript':
        text_table = 'publicationManuscript'
    elif text_type == 'variation':
        text_table = 'publicationVersion'
    elif text_type == 'commentary':
        text_table = 'publicationComment'
    elif text_type == 'facsimile':
        text_table = 'publicationFacsimile'

    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM {} WHERE id=:t_id".format(text_table))
    statement = sql.bindparams(t_id=text_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@digital_edition.route("/<project>/toc/<collection_id>")
def get_toc(project, collection_id):
    logger.info("Getting collection /{}/collection/{}".format(project, collection_id))

    file_path_query = safe_join(config[project]["file_root"], "toc", f'{collection_id}.json')

    try:
        file_path = [f for f in glob.iglob(file_path_query)][0]
        print(file_path)
        if os.path.exists(file_path):
            with io.open(file_path, encoding="UTF-8") as json_file:
                contents = json_file.read()
            return contents, 200
        else:
            abort(404)
    except Exception:
        print(file_path_query)
        abort(404)



@digital_edition.route("/<project>/collections")
def get_collections(project):
    logger.info("Getting collections /{}/collections".format(project))
    connection = db_engine.connect()
    status = 1 if config[project]["show_internally_published"] else 2

    sql = sqlalchemy.sql.text("SELECT id, name as title FROM publicationCollection WHERE published>=:p_status ORDER BY name")
    statement = sql.bindparams(p_status=status)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)

@digital_edition.route("/<project>/collection/<collection_id>")
def get_collection(project, collection_id):
    logger.info("Getting collection /{}/collection/{}".format(project, collection_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publicationCollection WHERE id=:c_id ORDER BY name")
    statement = sql.bindparams(c_id=collection_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)

@digital_edition.route("/<project>/publication/<publication_id>")
def get_publication(project, publication_id):
    logger.info("Getting publication /{}/publication/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE id=:p_id ORDER BY name")
    statement = sql.bindparams(p_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)

@digital_edition.route("/<project>/collection/<collection_id>/publications")
def get_collection_publications(project, collection_id):
    logger.info("Getting publication /{}/collections/{}/publications".format(project, collection_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE publicationCollection_id=:c_id ORDER BY id")
    statement = sql.bindparams(c_id=collection_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/inl")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/inl/<lang>")
def get_introduction(project, collection_id, publication_id, lang="swe"):
    """
    Get introduction text for a given publiction @TODO: remove publication_id, it is not needed.
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        version = "int" if config[project]["show_internally_published"] else "ext"
        filename = "{}_inl_{}_{}.xml".format(collection_id, lang, version)
        xsl_file = "est.xsl"
        content = get_content(project, "inl", filename, xsl_file, None)
        data = {
            "id": "{}_{}_inl".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/tit")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="swe"):
    """
    Get title page for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        version = "int" if config[project]["show_internally_published"] else "ext"
        filename = "{}_tit_{}_{}.xml".format(collection_id, lang, version)
        xsl_file = "title.xsl"
        content = get_content(project, "tit", filename, xsl_file, None)   
        data = {
            "id": "{}_{}_tit".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id):
    """
    Get reading text for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_est.xml".format(collection_id, publication_id)
        xsl_file = "est.xsl"
        content = get_content(project, "est", filename, xsl_file, None)
        data = {
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content.replace("id=", "data-id=")
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/com")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>")
def get_comments(project, collection_id, publication_id, note_id=None):
    """
    Get comments file text for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        filename = "{}_{}_com.xml".format(collection_id, publication_id)
        params = {
            "estDocument": '"file://{}"'.format(safe_join(config[project]["file_root"], "xml", "est", filename.replace("com", "est")))
        }
        if note_id is not None:
            params["noteId"] = '"{}"'.format(note_id)
            xsl_file = "notes.xsl"
        else:
            xsl_file = "com.xsl"

        content = get_content(project, "com", filename, xsl_file, params)
        data = {
            "id": "{}_{}_com".format(collection_id, publication_id),
            "content": content
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/ms/")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>")
def get_manuscript(project, collection_id, publication_id, manuscript_id=None):
    """
    Get one or all manuscripts for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        if manuscript_id is None:
            filename_search = "{}_{}_ms_%".format(collection_id, publication_id)
            select = "SELECT name, originalFilename, id FROM publicationManuscript WHERE originalFilename LIKE :query"
            statement = sqlalchemy.sql.text(select).bindparams(query=filename_search)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()
        else:
            select = "SELECT name, originalFilename, id FROM publicationManuscript WHERE id = :m_id"
            statement = sqlalchemy.sql.text(select).bindparams(m_id=manuscript_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()

        for index in range(len(manuscript_info)):
            manuscript = manuscript_info[index]
            params = {
                "bookId": collection_id
            }
            manuscript_info[index]["manuscript_changes"] = get_content(project, "ms", manuscript["originalFilename"], "ms_changes.xsl", params)
            manuscript_info[index]["manuscript_normalized"] = get_content(project, "ms", manuscript["originalFilename"], "ms_normalized.xsl", params)

        data = {
            "id": "{}_{}".format(collection_id, publication_id),
            "manuscripts": manuscript_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}_ms".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/var/")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a section (chapter)
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        filename_search = "{}_{}_var_%".format(collection_id, publication_id)
        if section_id is not None:
            select = "SELECT name, type, originalFilename, id FROM publicationVersion WHERE originalFilename LIKE :f_name AND section_id = :s_id"
            statement = sqlalchemy.sql.text(select).bindparams(f_name=filename_search, s_id=section_id)
        else:
            select = "SELECT name, type, originalFilename, id FROM publicationVersion WHERE originalFilename LIKE :f_name"
            statement = sqlalchemy.sql.text(select).bindparams(f_name=filename_search)
        variation_info = []
        for row in connection.execute(statement).fetchall():
            variation_info.append(dict(row))
        connection.close()

        for index in range(len(variation_info)):
            variation = variation_info[index]
            params = {
                "bookId": collection_id
            }

            if variation["type"] == 1:
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if section_id is not None:
                params["sectionId"] = section_id

            variation_info[index]["content"] = get_content(project, "var", variation["originalFilename"], xsl_file, params)

        data = {
            "id": "{}_{}_var".format(collection_id, publication_id),
            "variations": variation_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@digital_edition.route("/tooltips/subjects")
def subject_tooltips():
    """
    List all available subject tooltips as id and name
    """
    return jsonify(list_tooltips("subject"))


@digital_edition.route("/tooltips/tags")
def tag_tooltips():
    """
    List all available tag tooltips as id and name
    """
    return jsonify(list_tooltips("tag"))


@digital_edition.route("/tooltips/locations")
def location_tooltips():
    """
    List all available location tooltips as id and name
    """
    return jsonify(list_tooltips("location"))


@digital_edition.route("/tooltips/<object_type>/<ident>")
def get_tooltip_text(object_type, ident):
    """
    Get tooltip text for a specific subject, tag, or location
    object_type: one of "subject", "tag", "location"
    ident: legacy or numerical ID for desired object
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        return jsonify(get_tooltip(object_type, ident))

@digital_edition.route("/occurrences/<object_type>/<ident>")
def get_occurrences(object_type, ident):
    """
    Get event occurrence info and related publication IDs for a given subject, tag, or location
    Given a numerical or legacy ID for an object, returns a list of events and occurance information for the object
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        connection = db_engine.connect()
        try:
            object_id = int(ident)
        except ValueError:
            object_sql = "SELECT id FROM {} WHERE legacyId=:l_id".format(object_type)
            stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
            row = connection.execute(stmt).fetchone()
            object_id = row.id
        events_sql = "SELECT id, type, description FROM event WHERE id IN " \
                     "(SELECT event_id FROM eventConnection WHERE {}_id=:o_id)".format(object_type)
        occurrence_sql = "SELECT publication.publicationCollection_id AS collection_id, eventOccurrence.id, type, description, eventOccurrence.publication_id, eventOccurrence.publicationVersion_id, eventOccurrence.publicationFacsimile_id, eventOccurrence.publicationComment_id, eventOccurrence.publicationManuscript_id FROM eventOccurrence, publication WHERE eventOccurrence.event_id=:e_id AND eventOccurrence.publication_id=publication.id"

        events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
        results = []
        for row in connection.execute(events_stmnt).fetchall():
            results.append(dict(row))

        for event in results:
            event["occurrences"] = []
            occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
            for row in connection.execute(occurrence_stmnt).fetchall():
                event["occurrences"].append(dict(row))

        return jsonify(results)

@digital_edition.route("/occurrences/<object_type>")
def get_all_occurrences_by_type(object_type):
    """
    Get occurrences for each person
    TODO: refactor and divide into multiple functions
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        connection = db_engine.connect()
        ob_id = object_type + "_id"
        if object_type == "subject":
            name_attr = "fullName"
        else:
            name_attr = "name"

        ob_sql = "SELECT DISTINCT eventConnection.{}, {}.{} FROM digitalEdition.eventConnection, digitalEdition.eventOccurrence, digitalEdition.{} WHERE eventConnection.event_id=eventOccurrence.event_id AND eventConnection.{}={}.id"
        ob_sql = ob_sql.format(ob_id, object_type, name_attr, object_type, ob_id, object_type)
        ob_statement = sqlalchemy.sql.text(ob_sql)
        obs = []
        for row in connection.execute(ob_statement).fetchall():
            obs.append(dict(row))

        occur = []
        for o in obs:
            ident = o[ob_id]
            try:
                object_id = int(ident)
            except ValueError:
                object_sql = "SELECT id FROM {} WHERE legacyId=:l_id".format(object_type)
                stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
                row = connection.execute(stmt).fetchone()
                object_id = row.id
            events_sql = "SELECT id FROM event WHERE id IN " \
                        "(SELECT event_id FROM eventConnection WHERE {}_id=:o_id)".format(object_type)
            occurrence_sql = "SELECT publicationCollection.name AS collection_name, publication.publicationCollection_id AS collection_id, eventOccurrence.id, type, description, eventOccurrence.publication_id, eventOccurrence.publicationVersion_id, eventOccurrence.publicationFacsimile_id, eventOccurrence.publicationComment_id, eventOccurrence.publicationManuscript_id FROM eventOccurrence, publication, publicationCollection WHERE publication.publicationCollection_id=publicationCollection.id AND eventOccurrence.event_id=:e_id AND eventOccurrence.publication_id=publication.id"

            events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
            results = []
            for row in connection.execute(events_stmnt).fetchall():
                row = dict(row)
                if object_type == "subject":
                    type_stmnt = sqlalchemy.sql.text("SELECT type, subject.dateBorn, subject.dateDeceased FROM subject WHERE id=:ty_id").bindparams(ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    type_object = dict(type_object)
                    row["object_type"] = type_object["type"]
                    row["dateBorn"] = type_object["dateBorn"]
                    row["dateDeceased"] = type_object["dateDeceased"]
                results.append(row)

            # set occurrences for each object
            for event in results:
                event["occurrences"] = []
                occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
                for row in connection.execute(occurrence_stmnt).fetchall():
                    row = dict(row)

                    if row["publicationManuscript_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publicationManuscript.id AS id, publicationManuscript.originalFilename, publicationManuscript.name FROM publicationManuscript WHERE id={}".format(row["publicationManuscript_id"]))
                        manu = connection.execute(type_sql).fetchone()
                        row["publicationManuscript"] = dict(manu)
                    if row["publicationVersion_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publicationVersion.id AS id, publicationVersion.originalFilename, publicationVersion.name FROM publicationVersion WHERE id={}".format(row["publicationVersion_id"]))
                        variation = connection.execute(type_sql).fetchone()
                        row["publicationVersion"] = dict(variation)
                    if row["publicationComment_id"] is not None:
                        type_sql = ""
                    if row["publicationFacsimile_id"] is not None:
                        type_sql = "SELECT publicationFacsimile.id, publicationFacsimile.pageNr, publicationFacsimileCollection.title AS name, publicationFacsimile.sectionId, publicationFacsimileCollection.startPageNumber, publicationFacsimileCollection.folderPath, publicationFacsimileCollection.pageComment FROM publicationFacsimile, publicationFacsimileCollection WHERE publicationFacsimile.id={} AND publicationFacsimileCollection.id=publicationFacsimile.publicationFacsimileCollection_id".format(row["publicationFacsimile_id"])             
                        facs = connection.execute(type_sql).fetchone()
                        row["publicationFacsimile"] = dict(facs)
                    if row["publication_id"] is not None and row["publicationFacsimile_id"] is None and row["publicationFacsimile_id"] is None and row["publicationComment_id"] is None and row["publicationVersion_id"] is None and row["publicationManuscript_id"] is None:
                        type_sql = sqlalchemy.sql.text("SELECT publication.id AS publication_id, publication.originalFilename, publication.name FROM publication WHERE id={}".format(row["publication_id"]))
                        publication = connection.execute(type_sql).fetchone()
                        row["publication"] = dict(publication)

                    event["occurrences"].append(row)

            for i in results:
                if object_type == "subject":
                    i["name"] = o["fullName"]
                else:
                    i["name"] = o["name"]
                occur.append(i)

        return jsonify(occur)

@digital_edition.route("/<project>/occurrences/collection/<object_type>/<collection_id>")
def get_person_occurrences_by_collection(project, object_type, collection_id):
    connection = db_engine.connect()
    occurrence_sql = "SELECT publication.publicationCollection_id AS collection_id, eventOccurrence.id, eventOccurrence.event_id, type, description, eventOccurrence.publication_id, eventOccurrence.publicationVersion_id, eventOccurrence.publicationFacsimile_id, eventOccurrence.publicationComment_id, eventOccurrence.publicationManuscript_id FROM eventOccurrence, publication WHERE eventOccurrence.publication_id=publication.id AND publication.publicationCollection_id={} AND eventOccurrence.type='{}'".format(collection_id, object_type)

    occurrences = []
    for row in connection.execute(occurrence_sql).fetchall():
        occurrences.append(dict(row))

    subjects = []
    for occurrence in occurrences:
        subject_sql = "SELECT DISTINCT eventConnection.subject_id, subject.fullName, subject.legacyId, subject.project_id FROM eventOccurrence, eventConnection, subject WHERE eventOccurrence.event_id={} AND eventOccurrence.event_id = eventConnection.event_id AND eventConnection.subject_id =  subject.id".format(occurrence["event_id"])
        for row in connection.execute(subject_sql).fetchall():
            subjects.append(dict(row))
    connection.close()

    return jsonify(subjects)

# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/facsimiles/collections/<facsimile_collection_ids>")
def get_facsimile_collections(project, facsimile_collection_ids):
    logger.info("Getting facsimiles /{}/facsimiles/collections/{}".format(project, facsimile_collection_ids))
    connection = db_engine.connect()
    sql = """SELECT * FROM publicationFacsimileCollection where id in :ids"""
    statement = sqlalchemy.sql.text(sql).bindparams(ids=facsimile_collection_ids.split(','))
    return_data = []
    for row in connection.execute(statement).fetchall():
        return_data.append(dict(row))

    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/facsimiles/<publication_id>")
def get_facsimiles(project, publication_id):
    logger.info("Getting facsimiles /{}/facsimiles/{}".format(project, publication_id))

    connection = db_engine.connect()

    sql = """select * from publicationFacsimile as f
    left join publicationFacsimileCollection as fc on fc.id=f.publicationFacsimileCollection_id
    left join publication p on p.id=f.publication_id
    where f.publication_id=:p_id
    """

    if config[project]["show_internally_published"]:
        sql = " ".join([sql, "and p.published>0"])
    elif config[project]["show_unpublished"]:
        sql = " ".join([sql, "and p.published>2"])

    sql = " ".join([sql, "ORDER BY f.priority"])

    statement = sqlalchemy.sql.text(sql).bindparams(p_id=publication_id)

    images = {}
    result = []
    for row in connection.execute(statement).fetchall():
        facsimile = dict(row)
        if row.folderPath != '' and row.folderPath is not None:
            facsimile["start_url"] = row.folderPath
        else:
            facsimile["start_url"] = safe_join(
                    "digitaledition",
                    project,
                    "facsimile",
                    str(row["publicationFacsimileCollection_id"])
            )
        pre_pages = row["startPageNumber"] or 0

        facsimile["first_page"] = pre_pages + row["pageNr"]

        sql2 = "SELECT * FROM publicationFacsimile WHERE publicationFacsimileCollection_id=:fc_id AND pageNr>:pageNr ORDER BY pageNr ASC LIMIT 1"
        statement2 = sqlalchemy.sql.text(sql2).bindparams(fc_id=row["publicationFacsimileCollection_id"], pageNr=row["pageNr"])
        for row2 in connection.execute(statement2).fetchall():
            facsimile["last_page"] = pre_pages + row2["pageNr"] - 1

        if "last_page" not in facsimile.keys():
            facsimile["last_page"] = row["numberOfPages"]

        result.append(facsimile)
        '''try:
            with open(facsimile_image, "rb") as imageFile:
                facsimile["image_data"] = base64.b64encode(imageFile.read()).decode("utf-8")
        except:
            logger.error("Missing facsimile image: {}".format(facsimile_image))
        '''
    connection.close()

    return_data = result
    '''for row in result:
        if row["ed_id"] not in project_config.get(project).get("disabled_publications"):
            return_data.append(row)
    '''
    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


@digital_edition.route("/<project>/facsimiles/<collection_id>/<number>/<zoom_level>")
def get_facsimile_file(project, collection_id, number, zoom_level):
    """
    Retrieve a single facsimile image file from project root

    Facsimile files are stored as follows: root/facsimiles/<collection_id>/<zoom_level>/<page_number>.jpg
    The collection_id these are sorted by is the publicationFacsimileCollection id, stored as publication_id in the old database structure?

    However, the first page of a publication is not necessarily 1.jpg, as facsimiles often contain title pages and blank pages
    Thus, calling for facsimiles/1/1/1 may require fetching a file from root/facsimiles/1/1/5.jpg
    """
    # TODO published status for facsimile table to check against?
    # TODO S3 support
    connection = db_engine.connect()
    statement = sqlalchemy.sql.text("SELECT * FROM publicationFacsimileCollection WHERE id=:coll_id").bindparams(coll_id=collection_id)
    row = connection.execute(statement).fetchone()
    if row is None:
        return jsonify({
            "msg": "Desired facsimile collection was not found in database!"
        }), 404
    elif row.folderPath != '' and row.folderPath is not None:
        file_path = safe_join(row.folderPath, collection_id, zoom_level, "{}.jpg".format(int(number)))
    else:
        file_path = safe_join(config[project]["file_root"], "facsimiles", collection_id, zoom_level, "{}.jpg".format(int(number)))
    connection.close()

    output = io.BytesIO()
    with open(file_path, mode="rb") as img_file:
        output.write(img_file.read())
    content = output.getvalue()
    output.close()
    return Response(content, status=200, content_type="image/jpeg")


def list_tooltips(table):
    """
    List available tooltips for subjects, tags, or locations
    table should be 'subject', 'tag', or 'location'
    """
    if table not in ["subject", "tag", "location"]:
        return ""
    connection = db_engine.connect()
    if table == "subject":
        sql = sqlalchemy.sql.text("SELECT id, fullName, project_id, legacyId FROM subject")
    else:
        sql = sqlalchemy.sql.text("SELECT id, name, project_id, legacyId FROM {}".format(table))
    results = []
    for row in connection.execute(sql).fetchall():
        results.append(dict(row))
    connection.close()
    return results


def get_tooltip(table, row_id):
    """
    Get 'tooltip' style info for a single subject, tag, or location by its ID
    table should be 'subject', 'tag', or 'location'
    """
    connection = db_engine.connect()
    try:
        ident = int(row_id)
        is_legacy_id = False
    except ValueError:
        ident = row_id
        is_legacy_id = True
    if is_legacy_id:
        if table == "subject":
            sql = sqlalchemy.sql.text("SELECT id, legacyId, fullName, description FROM subject WHERE legacyId=:id")
        else:
            sql = sqlalchemy.sql.text("SELECT id, legacyId, name, description FROM {} WHERE legacyId=:id".format(table))
    else:
        if table == "subject":
            sql = sqlalchemy.sql.text("SELECT id, legacyId, fullName, description FROM subject WHERE id=:id")
        else:
            sql = sqlalchemy.sql.text("SELECT id, legacyId, name, description FROM {} WHERE id=:id".format(table))
    statement = sql.bindparams(id=ident)
    result = connection.execute(statement).fetchone()
    connection.close()
    return dict(result)


'''
    HELPER FUNCTIONS
'''


def slugify_route(path):
    path = path.replace(" - ", "")
    path = path.replace(" ", "-")
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('[^a-zA-Z0-9\\\/-]|_', '', re.sub('.md', '', path))
    return path.lower()


def slugify_id(path, language):
    path = re.sub('[^0-9]', '', path)
    path = language + path
    path = '-'.join(path[i:i+2] for i in range(0, len(path), 2))
    return path


def slugify_path(path):
    path = split_after(path, "/topelius_required/md/")
    return re.sub('.md', '', path)


def path_hierarchy(path, language):
    hierarchy = {'id': slugify_id(path, language), 'title': filter_title(os.path.basename(path)),
                 'basename': re.sub('.md', '', os.path.basename(path)), 'path': slugify_path(path), 'fullpath': path,
                 'route': slugify_route(split_after(path, "/topelius_required/md/")), 'type': 'folder',
                 'children': [path_hierarchy(p, language) for p in glob.glob(os.path.join(path, '*'))]}

    if not hierarchy['children']:
        del hierarchy['children']
        hierarchy['type'] = 'file'

    return dict(hierarchy)


def filter_title(path):
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('-', '', path)
    path = re.sub('.md', '', path)
    return path.strip()


def split_after(value, a):
    pos_a = value.rfind(a)
    if pos_a == -1:
        return ""
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= len(value):
        return ""
    return value[adjusted_pos_a:]


def cache_is_recent(source_file, xsl_file, cache_file):
    """
    Returns False if the source or xsl file have been modified since the creation of the cache file
    Returns False if the cache is more than 'cache_lifetime_seconds' seconds old, as defined in config file
    Otherwise, returns True
    """
    try:
        source_file_mtime = os.path.getmtime(source_file)
        xsl_file_mtime = os.path.getmtime(xsl_file)
        cache_file_mtime = os.path.getmtime(cache_file)
    except OSError:
        return False
    if source_file_mtime > cache_file_mtime or xsl_file_mtime > cache_file_mtime:
        return False
    elif calendar.timegm(time.gmtime()) > (cache_file_mtime + config["cache_lifetime_seconds"]):
        return False
    return True


def get_published_status(project, collection_id, publication_id):
    """
    Returns info on if project, publicationCollection, and publication are all published
    Returns three values:
        - a boolean if the publication can be shown
        - a message text why it can't be shown, if that is the case
        -

    Publications can be shown if they're externally published (published==2),
    if they're internally published (published==1) and show_internally_published is True,
    or if FLASK_DEBUG is set to 1 (all publications are shown in DEBUG mode
    """
    connection = db_engine.connect()
    select = """SELECT project.published AS proj_pub, publicationCollection.published AS col_pub, publication.published as pub 
    FROM project JOIN publicationCollection JOIN publication
    WHERE project.id = publicationCollection.project_id
    AND publication.publicationCollection_id = publicationCollection.id
    AND project.name = :project AND publicationCollection.id = :c_id AND publication.id = :p_id
    """
    if int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        can_show = True
        message = ""
    else:
        statement = sqlalchemy.sql.text(select).bindparams(project=project, c_id=collection_id, p_id=publication_id)
        result = connection.execute(statement)
        show_internal = config[project]["show_internally_published"]
        can_show = False
        message = ""
        row = result.fetchone()
        if row is None:
            message = "Content does not exist"
        else:
            status = min(row.proj_pub, row.col_pub, row.pub)
            if status < 1:
                message = "Content is not published"
            elif status == 1 and not show_internal:
                message = "Content is not externally published"
            else:
                can_show = True
    return can_show, message


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def xml_to_html(xsl_file_path, xml_file_path, replace_namespace=True, params=None):
    logger.debug("Transforming {} using {}".format(xml_file_path, xsl_file_path))
    if params is not None:
        logger.debug("Parameters are {}".format(params))
    if not os.path.exists(xsl_file_path):
        return "XSL file {!r} not found!".format(xsl_file_path)
    if not os.path.exists(xml_file_path):
        return "XML file {!r} not found!".format(xml_file_path)

    with io.open(xml_file_path, mode="rb") as xml_file:
        xml_contents = xml_file.read()
        if replace_namespace:
            xml_contents = xml_contents.replace(b'xmlns="http://www.sls.fi/tei"', b'xmlns="http://www.tei-c.org/ns/1.0"')

        xml_root = etree.fromstring(xml_contents)

    xsl_parser = etree.XMLParser()
    xsl_parser.resolvers.add(FileResolver())
    with io.open(xsl_file_path, encoding="UTF-8") as xsl_file:
        xslt_root = etree.parse(xsl_file, parser=xsl_parser)
        xsl_transform = etree.XSLT(xslt_root)

    if params is None:
        result = xsl_transform(xml_root)
    elif isinstance(params, dict) or isinstance(params, OrderedDict):
        result = xsl_transform(xml_root, **params)
    else:
        raise Exception("Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {}".format(type(params)))
    if len(xsl_transform.error_log) > 0:
        logging.debug(xsl_transform.error_log)
    return str(result)


def get_content(project, folder, xml_filename, xsl_filename, parameters):
    xml_file_path = safe_join(config[project]["file_root"], "xml", folder, xml_filename)
    xsl_file_path = safe_join(config[project]["file_root"], "xslt", xsl_filename)
    cache_file_path = xml_file_path.replace("/xml/", "/cache/").replace(".xml", ".html")
    content = None

    if parameters is not None:
        if 'noteId' in parameters:
            note_file_name = xml_filename.split(".xml")[0] + "_" + parameters["noteId"]
            cache_file_path = cache_file_path.replace(xml_filename.split(".xml")[0], note_file_name)
            cache_file_path = cache_file_path.replace('"', '')
            cache_file_note_path_copy = cache_file_path
            if not os.path.exists(cache_file_path):
                cache_file_path = ""

    if os.path.exists(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        else:
            logger.info("Cache file is old or invalid, deleting cache file...")
            os.remove(cache_file_path)
    if os.path.exists(xml_file_path) and content is None:
        logger.info("Getting contents from file and transforming...")
        try:
            content = xml_to_html(xsl_file_path, xml_file_path, params=parameters).replace('\n', '').replace('\r', '')
            try:
                if parameters is not None:
                    if 'noteId' in parameters:
                        cache_file_path = cache_file_note_path_copy
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        cache_file.write(content)
            except Exception:
                logger.exception("Could not create cachefile")
                content = "Successfully fetched content but could not generate cache for it."
        except Exception as e:
            logger.exception("Error when parsing XML file")
            content = "Error parsing document"
            content += str(e)
    elif content is None:
        content = "File not found"

    return content
