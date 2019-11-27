import calendar
from collections import OrderedDict
from flask import abort, Blueprint, request, Response, safe_join, send_file
from flask.json import jsonify
from flask_jwt_extended import jwt_optional, get_jwt_identity
import io
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import os
import sqlalchemy.sql
import time
import re
import urllib
import glob
import json
from PIL import Image
from hashlib import md5
from elasticsearch import Elasticsearch

from sls_api.endpoints.generics import config, get_project_config, db_engine, select_all_from_table, elastic_config, get_project_id_from_name

digital_edition = Blueprint('digital_edition', __name__)

logger = logging.getLogger("sls_api.digital_edition")

es = Elasticsearch([{'host': elastic_config['host'], 'port': elastic_config['port']}])
es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler(filename=config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)


@digital_edition.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, PUT"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response


@digital_edition.route("/projects/")
@jwt_optional
def get_projects():
    """
    List all GDE projects
    """
    jwt = get_jwt_identity()
    if jwt is None:
        return select_all_from_table("project")
    else:
        if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and jwt["sub"] == "test@test.com":
            # test user in DEBUG mode has access to all projects
            return select_all_from_table("project")
        else:
            return jsonify(jwt["projects"])


@digital_edition.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting static content from /{}/html/{}".format(project, filename))
        file_path = safe_join(config["file_root"], "html", "{}.html".format(filename))
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
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        parts = fileid.split("-")
        pathTmp = fileid
        if len(parts) > 4:
            pathTmp = parts[0] + "-" + parts[1] + "-" + parts[2] + "-0" + parts[4]
        path = "*/".join(pathTmp.split("-")) + "*"

        file_path_query = safe_join(config["file_root"], "md", path)

        try:
            file_path_full = [f for f in glob.iglob(file_path_query)]
            if len(file_path_full) <= 0:
                abort(404)
            else:
                file_path = file_path_full[0]
                logger.info("Finding {} (md_contents fetch)".format(file_path))
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
            logger.exception("Error fetching: {}".format(file_path_query))
            abort(404)


@digital_edition.route("/<project>/static-pages-toc/<language>/sort")
@digital_edition.route("/<project>/static-pages-toc/<language>")
def get_static_pages_as_json(project, language):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting static content from /{}/static-pages-toc/{}".format(project, language))
        folder_path = safe_join(config["file_root"], "md", language)

        if os.path.exists(folder_path):
            data = path_hierarchy(project, folder_path, language)
            return jsonify(data), 200
        else:
            logger.info("did not find {}".format(folder_path))
            abort(404)


@digital_edition.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text('SELECT * FROM publication_manuscript WHERE publication_id=:pub_id')
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
        text_table = 'publication_manuscript'
    elif text_type == 'variation':
        text_table = 'publication_version'
    elif text_type == 'commentary':
        text_table = 'publication_comment'
    elif text_type == 'facsimile':
        text_table = 'publication_facsimile'

    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM {} WHERE id=:t_id".format(text_table))
    statement = sql.bindparams(t_id=text_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


def getFacsimileImage(project, edition_id, publication_id, size=(300, 300)):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimile image from funciton getFacsimileImage({},{},{})".format(project, edition_id, publication_id))

        outfile = md5("{}-{}-{}-{}".format(edition_id, publication_id, size[0], size[1]).encode('utf-8'))
        cache_file_path = safe_join(config["file_root"], "cache", "faksimil", "{}.png".format(outfile))
        image_file_path = safe_join(config["file_root"], "faksimil", edition_id, "{}.png".format(publication_id))
        if os.path.exists(cache_file_path):
            logger.debug("cache_file_path exists: {}".format(cache_file_path))
            return cache_file_path
        else:
            try:
                logger.debug("cache_file_path does not exist: {}".format(cache_file_path))
                logger.debug("Will create new image")
                im = Image.open(image_file_path)
                im.thumbnail(size, Image.ANTIALIAS)
                im.save(cache_file_path, "JPEG")
                logger.debug("I think it was successful")
                return cache_file_path
            except Exception:
                logger.exception("Exception when creating image cache file.")
                return ""


@digital_edition.route("/<project>/facsimiles/<publication_id>")
def get_facsimiles(project, publication_id):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimiles /{}/facsimiles/{}".format(project, publication_id))

        connection = db_engine.connect()

        sql = 'select * from publication_facsimile as f \
        left join publication_facsimile_collection as fc on fc.id=f.publication_facsimile_collection_id \
        left join publication p on p.id=f.publication_id \
        where f.publication_id=:p_id \
        '

        if config["show_internally_published"]:
            sql = " ".join([sql, "and p.published>0"])
        elif config["show_unpublished"]:
            sql = " ".join([sql, "and p.published>2"])

        sql = " ".join([sql, "ORDER BY f.priority"])

        pub_id = publication_id.split('_')[1]

        statement = sqlalchemy.sql.text(sql).bindparams(p_id=pub_id)

        result = []
        for row in connection.execute(statement).fetchall():
            facsimile = dict(row)
            if row.folder_path != '' and row.folder_path is not None:
                facsimile["start_url"] = row.folder_path
            else:
                facsimile["start_url"] = safe_join(
                    "digitaledition",
                    project,
                    "facsimile",
                    str(row["publication_facsimile_collection_id"])
                )
            pre_pages = row["start_page_number"] or 0

            facsimile["first_page"] = pre_pages + row["page_nr"]

            sql2 = "SELECT * FROM publication_facsimile WHERE publication_facsimile_collection_id=:fc_id AND page_nr>:page_nr ORDER BY page_nr ASC LIMIT 1"
            statement2 = sqlalchemy.sql.text(sql2).bindparams(fc_id=row["publication_facsimile_collection_id"], page_nr=row["page_nr"])
            for row2 in connection.execute(statement2).fetchall():
                facsimile["last_page"] = pre_pages + row2["page_nr"] - 1

            if "last_page" not in facsimile.keys():
                facsimile["last_page"] = row["number_of_pages"]

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


@digital_edition.route("/<project>/toc/<collection_id>", methods=["GET", "PUT"])
@jwt_optional
def handle_toc(project, collection_id):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        if request.method == "GET":
            logger.info(f"Getting table of contents for /{project}/toc/{collection_id}")
            file_path_query = safe_join(config["file_root"], "toc", f'{collection_id}.json')

            try:
                file_path = [f for f in glob.iglob(file_path_query)][0]
                logger.info(f"Finding {file_path} (toc collection fetch)")
                if os.path.exists(file_path):
                    with io.open(file_path, encoding="UTF-8") as json_file:
                        contents = json_file.read()
                    return contents, 200
                else:
                    abort(404)
            except IndexError:
                logger.warning(f"File {file_path_query} not found on disk.")
                abort(404)
            except Exception:
                logger.exception(f"Error fetching {file_path_query}")
                abort(404)
        elif request.method == "PUT":
            # uploading a new table of contents requires authorization and project permission
            identity = get_jwt_identity()
            if identity is None:
                return jsonify({"msg": "Missing Authorization Header"}), 403
            else:
                authorized = False
                # in debug mode, test user has access to every project
                if int(os.environ.get("FLASK_DEBUG", 0)) == 1 and identity["sub"] == "test@test.com":
                    authorized = True
                elif project in identity["projects"]:
                    authorized = True

                if not authorized:
                    return jsonify({"msg": "No access to this project."}), 403
                else:
                    logger.info(f"Processing new table of contents for /{project}/toc/{collection_id}")
                    data = request.get_json()
                    if not data:
                        return jsonify({"msg": "No JSON in payload."}), 400
                    file_path = safe_join(config["file_root"], "toc", f"{collection_id}.json")
                    try:
                        # save new toc as file_path.new
                        with open(f"{file_path}.new", "w", encoding="utf-8") as outfile:
                            json.dump(data, outfile)
                    except Exception as ex:
                        # if we fail to save the file, make sure it doesn't exist before returning an error
                        try:
                            os.remove(f"{file_path}.new")
                        except FileNotFoundError:
                            pass
                        return jsonify({"msg": "Failed to save JSON data to disk.", "reason": ex}), 500
                    else:
                        # if we succeed, remove the old file and rename file_path.new to file_path
                        # (could be combined into just os.rename, but some OSes don't like that)
                        os.rename(f"{file_path}.new", file_path)
                        return jsonify({"msg": f"Saved new toc as {file_path}"})


@digital_edition.route("/<project>/collections")
def get_collections(project):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting collections /{}/collections".format(project))
        connection = db_engine.connect()
        status = 1 if config["show_internally_published"] else 2
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text("SELECT id, name as title FROM publication_collection WHERE project_id = :p_id AND published>=:p_status ORDER BY name")
        statement = sql.bindparams(p_status=status, p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results)

@digital_edition.route("/<project>/publication-facsimile-relations/")
def get_project_publication_facsimile_relations(project):
    logger.info("Getting publication relations for {}".format(project))
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    sql = sqlalchemy.sql.text(
        "SELECT pc.id as pc_id, p.id as p_id, pf.id as pf_id,\
         pc.name as pc_name, p.name as p_name, pf.page_nr FROM publication_collection pc \
         JOIN publication p ON p.publication_collection_id=pc.id \
         JOIN publication_facsimile pf ON pf.publication_id = p.id \
         WHERE project_id=:p_id ORDER BY pc.id")
    statement = sql.bindparams(p_id=project_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)

@digital_edition.route("/<project>/collection/<collection_id>")
def get_collection(project, collection_id):
    logger.info("Getting collection /{}/collection/{}".format(project, collection_id))
    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM publication_collection WHERE id=:c_id ORDER BY name")
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
    sql = sqlalchemy.sql.text("SELECT * FROM publication WHERE publication_collection_id=:c_id ORDER BY id")
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
    Get introduction text for a given publication @TODO: remove publication_id, it is not needed.
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_published_status(project, collection_id, publication_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_introduction table? how handle language/version
            filename = "{}_inl_{}_{}.xml".format(collection_id, lang, version)
            xsl_file = "introduction.xsl"
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
    Get title page for a given publication @TODO: remove publication_id, it is not needed?
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_title_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_title table? how handle language/version
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


@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/est/<section_id>")
@digital_edition.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id, section_id=None):
    """
    Get reading text for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT legacy_id FROM publication WHERE id = :p_id"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()
        if not result[0]:
            filename = "{}_{}_est.xml".format(collection_id, publication_id)
            connection.close()
        else:
            filename = "{}_est.xml".format(result["legacy_id"])
            connection.close()
        logger.debug("Filename (est) for {} is {}".format(publication_id, filename))
        xsl_file = "est.xsl"
        if section_id is not None:
            section_id = '"{}"'.format(section_id)
            content = get_content(project, "est", filename, xsl_file, {"bookId": collection_id, "sectionId": section_id})
        else:
            content = get_content(project, "est", filename, xsl_file, {"bookId": collection_id})
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
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_published_status(project, collection_id, publication_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            connection = db_engine.connect()
            select = "SELECT legacy_id FROM publication_comment WHERE id IN (SELECT publication_comment_id FROM publication WHERE id = :p_id) AND legacy_id IS NOT NULL"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            result = connection.execute(statement).fetchone()
            if result is not None:
                filename = "{}_com.xml".format(result["legacy_id"])
                connection.close()
            else:
                filename = "{}_{}_com.xml".format(collection_id, publication_id)
                connection.close()
            logger.debug("Filename (com) for {} is {}".format(publication_id, filename))
            params = {
                "estDocument": '"file://{}"'.format(safe_join(config["file_root"], "xml", "est", filename.replace("com", "est"))),
                "bookId": collection_id
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
            connection.close()
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
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()
        else:
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE id = :m_id ORDER BY sort_order ASC"
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
            if manuscript["legacy_id"] is not None:
                filename = "{}.xml".format(manuscript["legacy_id"])
            else:
                filename = "{}_{}_ms_{}.xml".format(collection_id, publication_id, manuscript["id"])
            manuscript_info[index]["manuscript_changes"] = get_content(project, "ms", filename, "ms_changes.xsl", params)
            manuscript_info[index]["manuscript_normalized"] = get_content(project, "ms", filename, "ms_normalized.xsl", params)

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
        if section_id is not None:
            select = "SELECT sort_order, name, type, legacy_id, id, original_filename FROM publication_version WHERE publication_id = :p_id AND section_id = :s_id ORDER BY type, sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id, s_id=section_id)
        else:
            select = "SELECT sort_order, name, type, legacy_id, id, original_filename FROM publication_version WHERE publication_id = :p_id ORDER BY type, sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
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
                params["section_id"] = section_id

            if variation["legacy_id"] is not None:
                filename = "{}.xml".format(variation["legacy_id"])
            else:
                filename = "{}_{}_var_{}.xml".format(collection_id, publication_id, variation["id"])

            variation_info[index]["content"] = get_content(project, "var", filename, xsl_file, params)

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


@digital_edition.route("/<project>/tooltips/<object_type>/<ident>/")
@digital_edition.route("/<project>/tooltips/<object_type>/<ident>/<use_legacy>/")
def get_project_tooltip_text(project, object_type, ident, use_legacy=False):
    """
    Get tooltip text for a specific subject, tag, or location
    object_type: one of "subject", "tag", "location"
    ident: legacy or numerical ID for desired object
    """
    if object_type not in ["subject", "tag", "location"]:
        abort(404)
    else:
        return jsonify(get_tooltip(object_type, ident, project, use_legacy))


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
            object_sql = "SELECT id FROM {} WHERE legacy_id=:l_id".format(object_type)
            stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
            row = connection.execute(stmt).fetchone()
            if row is not None:
                object_id = row.id
            else:
                connection.close()
                return jsonify([])

        events_sql = "SELECT id, type, description FROM event WHERE id IN " \
                     "(SELECT event_id FROM event_connection WHERE {}_id=:o_id)".format(object_type)
        occurrence_sql = "SELECT original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                        variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                        recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                        performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, note as song_note, comment as song_comment, \
                        lyrics as song_lyrics, original_collection_location as song_original_collection_location, original_collection_signature as song_original_collection_signature,\
                        ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype, event_occurrence.id,\
                        publication.publication_collection_id AS collection_id, event_occurrence.id, event_occurrence.type, description, \
                        event_occurrence.publication_id, event_occurrence.publication_version_id, event_occurrence.publication_facsimile_id, \
        event_occurrence.publication_comment_id, event_occurrence.publication_manuscript_id, \
        pc.name as publication_collection_name, publication.name as publication_name \
        FROM event_occurrence, publication \
        JOIN publication_collection pc ON pc.id = publication.publication_collection_id \
        LEFT OUTER JOIN publication_song ps ON ps.publication_id = publication.id \
        WHERE event_occurrence.event_id=:e_id AND event_occurrence.publication_id=publication.id \
        AND (event_occurrence.publication_song_id = ps.id OR event_occurrence.publication_song_id is null)"

        events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
        results = []
        for row in connection.execute(events_stmnt).fetchall():
            results.append(dict(row))

        for event in results:
            event["occurrences"] = []
            occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
            for row in connection.execute(occurrence_stmnt).fetchall():
                event["occurrences"].append(dict(row))
        connection.close()
        return jsonify(results)


@digital_edition.route("/<project>/occurrences/<object_type>")
@digital_edition.route("/occurrences/<object_type>")
def get_all_occurrences_by_type(object_type, project=None):
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
            name_attr = "full_name"
        else:
            name_attr = "name"

        if project is None:
            ob_sql = "SELECT DISTINCT event_connection.{}, {}.{} FROM event_connection, event_occurrence, {} \
            WHERE event_connection.event_id=event_occurrence.event_id AND event_connection.{}={}.id"
            ob_sql = ob_sql.format(ob_id, object_type, name_attr, object_type, ob_id, object_type)
        else:
            project_id = get_project_id_from_name(project)
            ob_sql = "SELECT DISTINCT event_connection.{}, {}.{} FROM event_connection, event_occurrence, {} \
            WHERE event_connection.event_id=event_occurrence.event_id AND event_connection.{}={}.id AND {}.project_id={}"
            ob_sql = ob_sql.format(ob_id, object_type, name_attr, object_type, ob_id, object_type, object_type, project_id)

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
                object_sql = "SELECT id FROM {} WHERE legacy_id=:l_id".format(object_type)
                stmt = sqlalchemy.sql.text(object_sql).bindparams(l_id=ident)
                row = connection.execute(stmt).fetchone()
                object_id = row.id
            events_sql = "SELECT id FROM event WHERE id IN " \
                         "(SELECT event_id FROM event_connection WHERE {}_id=:o_id)".format(object_type)
            occurrence_sql = "SELECT publication_collection.name AS collection_name, publication.publication_collection_id AS collection_id,\
             event_occurrence.id, type, description, event_occurrence.publication_id, event_occurrence.publication_version_id,\
             event_occurrence.publication_facsimile_id, event_occurrence.publication_comment_id, event_occurrence.publication_facsimile_page, \
             event_occurrence.publication_manuscript_id, event_occurrence.publication_song_id FROM event_occurrence, publication, publication_collection \
             WHERE publication.publication_collection_id=publication_collection.id AND event_occurrence.event_id=:e_id AND event_occurrence.publication_id=publication.id"

            events_stmnt = sqlalchemy.sql.text(events_sql).bindparams(o_id=object_id)
            results = []
            for row in connection.execute(events_stmnt).fetchall():
                row = dict(row)
                if object_type == "subject":
                    type_stmnt = sqlalchemy.sql.text("SELECT type, subject.first_name::text, subject.last_name::text, subject.source::text, subject.description::text, subject.occupation::text, subject.place_of_birth::text, subject.date_born::text, subject.date_deceased::text FROM subject WHERE id=:ty_id").bindparams(ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    type_object = dict(type_object)
                    row["object_type"] = type_object["type"]
                    row["date_born"] = type_object["date_born"]
                    row["date_deceased"] = type_object["date_deceased"]
                    row["first_name"] = type_object["first_name"]
                    row["last_name"] = type_object["last_name"]
                    row["source"] = type_object["source"]
                    row["description"] = type_object["description"]
                    row["occupation"] = type_object["occupation"]
                    row["place_of_birth"] = type_object["place_of_birth"]
                if object_type == "tag":
                    type_stmnt = sqlalchemy.sql.text("SELECT tag.type::text, tag.description::text, tag.source::text, tag.name::text FROM tag WHERE id=:ty_id").bindparams(ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    type_object = dict(type_object)
                    row["description"] = type_object["description"]
                    row["source"] = type_object["source"]
                    row["name"] = type_object["name"]
                    row["type"] = type_object["type"]
                if object_type == "location":
                    type_stmnt = sqlalchemy.sql.text("SELECT location.description::text, location.source::text, location.name::text, location.country::text, location.city::text, \
                                                        location.latitude::text, location.longitude::text, location.region::text FROM location WHERE id=:ty_id").bindparams(ty_id=object_id)
                    type_object = connection.execute(type_stmnt).fetchone()
                    type_object = dict(type_object)
                    row["description"] = type_object["description"]
                    row["source"] = type_object["source"]
                    row["name"] = type_object["name"]
                    row["country"] = type_object["country"]
                    row["city"] = type_object["city"]
                    row["latitude"] = type_object["latitude"]
                    row["longitude"] = type_object["longitude"]
                    row["region"] = type_object["region"]
                results.append(row)

            # set occurrences for each object
            for event in results:
                event["occurrences"] = []
                occurrence_stmnt = sqlalchemy.sql.text(occurrence_sql).bindparams(e_id=event["id"])
                for row in connection.execute(occurrence_stmnt).fetchall():
                    row = dict(row)

                    if row["publication_manuscript_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publication_manuscript.id AS id, publication_manuscript.original_filename, publication_manuscript.name \
                        FROM publication_manuscript WHERE id={}".format(row["publication_manuscript_id"]))
                        manu = connection.execute(type_sql).fetchone()
                        row["publication_manuscript"] = dict(manu)
                    if row["publication_version_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT publication_version.id AS id, publication_version.original_filename, publication_version.name \
                        FROM publication_version WHERE id={}".format(row["publication_version_id"]))
                        variation = connection.execute(type_sql).fetchone()
                        row["publication_version"] = dict(variation)
                    if row["publication_comment_id"] is not None:
                        type_sql = ""
                    if row["publication_facsimile_id"] is not None:
                        type_sql = "SELECT publication_facsimile.id, publication_facsimile.page_nr, publication_facsimile_collection.title AS name, \
                        publication_facsimile.section_id, publication_facsimile_collection.start_page_number, publication_facsimile_collection.folder_path, \
                        publication_facsimile_collection.page_comment FROM publication_facsimile, publication_facsimile_collection \
                        WHERE publication_facsimile.id={} AND \
                        publication_facsimile_collection.id=publication_facsimile.publication_facsimile_collection_id".format(row["publication_facsimile_id"])
                        facs = connection.execute(type_sql).fetchone()
                        row["publication_facsimile"] = dict(facs)
                    if row["publication_id"] is not None \
                            and row["publication_facsimile_id"] is None \
                            and row["publication_facsimile_id"] is None \
                            and row["publication_comment_id"] is None \
                            and row["publication_version_id"] is None \
                            and row["publication_manuscript_id"] is None:
                        type_sql = sqlalchemy.sql.text("SELECT publication.id AS publication_id, publication.original_filename, publication.name FROM publication WHERE id=:pub_id").bindparams(pub_id=row["publication_id"])
                        publication = connection.execute(type_sql).fetchone()
                        row["publication"] = dict(publication)
                    if row["publication_song_id"] is not None:
                        type_sql = sqlalchemy.sql.text("SELECT \
                            original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                        variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                        recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                        performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, note as song_note, comment as song_comment, \
                        lyrics as song_lyrics, original_collection_location as song_original_collection_location, original_collection_signature as song_original_collection_signature,\
                        ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype \
                         FROM publication_song WHERE id=:song_id").bindparams(song_id=row["publication_song_id"])
                        publication_song = connection.execute(type_sql).fetchone()
                        row["publication_song"] = dict(publication_song)

                    event["occurrences"].append(row)

            for i in results:
                if object_type == "subject":
                    i["name"] = o["full_name"]
                else:
                    i["name"] = o["name"]
                occur.append(i)
        connection.close()
        return jsonify(occur)


@digital_edition.route("/<project>/subject/occurrences/<subject_id>/")
@digital_edition.route("/<project>/subject/occurrences/")
def get_subject_occurrences(project=None, subject_id=None):
    if project == 'all':
        subject_sql = " SELECT id, date_born::text, date_deceased::text, description, first_name, last_name, full_name as name, \
                        type as object_type, occupation, place_of_birth, source \
                        FROM subject WHERE deleted != 1"
        if subject_id is not None:
            subject_sql = subject_sql + " AND id = :sub_id "
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(sub_id=subject_id)
        else:
            statement_subj = sqlalchemy.sql.text(subject_sql)
    else:
        subject_sql = " SELECT id, date_born::text, date_deceased::text, description, first_name, last_name, full_name as name, \
                        type as object_type, occupation, place_of_birth, source \
                        FROM subject WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if subject_id is not None:
            subject_sql = subject_sql + " AND id = :sub_id "
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(sub_id=subject_id, project_id=project_id)
        else:
            statement_subj = sqlalchemy.sql.text(subject_sql).bindparams(project_id=project_id)

    connection = db_engine.connect()
    subjects = []
    result = connection.execute(statement_subj)
    subject = result.fetchone()
    while subject is not None:
        subject = dict(subject)
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN subject sub ON ev_c.subject_id = sub.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND sub.id = :sub_id ORDER BY pub_c.name ASC "

        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(sub_id=subject['id'])
        subject['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = dict(occurrence)
            if subject_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrence['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = dict(song_data)
                    occurrenceData.update(song_data)
            subject['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(subject['occurrences']) > 0:
            subjects.append(subject)

        connection_2.close()
        subject = result.fetchone()
    connection.close()

    return jsonify(subjects)

@digital_edition.route("/<project>/location/occurrences/<location_id>/")
@digital_edition.route("/<project>/location/occurrences/")
def get_location_occurrences(project=None, location_id=None):
    if project == 'all':
        location_sql = " SELECT id, city, country, description, latitude, longitude, name, region, source \
                        FROM location WHERE deleted != 1"
        if location_id is not None:
            location_sql = location_sql + " AND id = :location_id "
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(location_id=location_id)
        else:
            statement_loc = sqlalchemy.sql.text(location_sql)
    else:
        location_sql = " SELECT id, city, country, description, latitude, longitude, name, region, source \
                        FROM location WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if location_id is not None:
            location_sql = location_sql + " AND id = :location_id "
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(location_id=location_id, project_id=project_id)
        else:
            statement_loc = sqlalchemy.sql.text(location_sql).bindparams(project_id=project_id)


    connection = db_engine.connect()
    locations = []
    result = connection.execute(statement_loc)
    location = result.fetchone()
    while location is not None:
        location = dict(location)
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN location loc ON ev_c.location_id = loc.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND loc.id = :loc_id ORDER BY pub_c.name ASC"
        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(loc_id=location['id'])
        location['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = dict(occurrence)
            if location_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrence['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = dict(song_data)
                    occurrenceData.update(song_data)
            location['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(location['occurrences']) > 0:
            locations.append(location)

        connection_2.close()
        location = result.fetchone()
    connection.close()

    return jsonify(locations)

@digital_edition.route("/<project>/tag/occurrences/<tag_id>/")
@digital_edition.route("/<project>/tag/occurrences/")
def get_tag_occurrences(project=None, tag_id=None):
    if project == 'all':
        tag_sql = " SELECT id, type, name, description, source \
                        FROM tag WHERE deleted != 1"
        if tag_id is not None:
            tag_sql = tag_sql + " AND id = :tag_id "
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(tag_id=tag_id)
        else:
            statement_tag = sqlalchemy.sql.text(tag_sql)
    else:
        tag_sql = " SELECT id, type, name, description, source \
                        FROM tag WHERE deleted != 1 AND project_id = :project_id"
        project_id = get_project_id_from_name(project)
        if tag_id is not None:
            tag_sql = tag_sql + " AND id = :tag_id "
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(tag_id=tag_id, project_id=project_id)
        else:
            statement_tag = sqlalchemy.sql.text(tag_sql).bindparams(project_id=project_id)

    connection = db_engine.connect()
    tags = []
    result = connection.execute(statement_tag)
    tag = result.fetchone()
    while tag is not None:
        tag = dict(tag)
        occurrence_sql = "SELECT \
                            pub_c.name as collection_name, pub_c.id as collection_id, ev.description, ev.id, ev_o.publication_comment_id, \
                            publication_facsimile_id, publication_facsimile_page, \
                            publication_manuscript_id, publication_version_id, ev.type, \
                            pub.id as publication_id, pub.name as publication_name, pub.original_filename as original_filename, \
                            ev_o.publication_song_id as publication_song_id \
                            FROM event_connection ev_c \
                            JOIN event ev ON ev.id = ev_c.event_id \
                            JOIN event_occurrence ev_o ON ev_o.event_id = ev_c.event_id \
                            JOIN publication pub ON pub.id = ev_o.publication_id \
                            JOIN publication_collection pub_c ON pub_c.id = pub.publication_collection_id \
                            JOIN tag ON ev_c.tag_id = tag.id \
                            WHERE ev.deleted != 1 AND ev_o.deleted != 1 AND ev_c.deleted != 1 AND tag.id = :tag_id ORDER BY pub_c.name ASC"
        statement_occ = sqlalchemy.sql.text(occurrence_sql).bindparams(tag_id=tag['id'])
        tag['occurrences'] = []
        connection_2 = db_engine.connect()
        result_2 = connection_2.execute(statement_occ)
        occurrence = result_2.fetchone()
        while occurrence is not None:
            occurrenceData = dict(occurrence)
            if tag_id is not None:
                song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype\
                FROM publication_song ps WHERE ps.id = :song_id\
                ORDER BY ps.type ASC"
                song_sql = sqlalchemy.sql.text(song_sql).bindparams(song_id=occurrence['publication_song_id'])
                song_result = connection_2.execute(song_sql)
                song_data = song_result.fetchone()
                if song_data is not None:
                    song_data = dict(song_data)
                    occurrenceData.update(song_data)
            tag['occurrences'].append(occurrenceData)
            occurrence = result_2.fetchone()

        if len(tag['occurrences']) > 0:
            tags.append(tag)
            
        connection_2.close()
        tag = result.fetchone()

    connection.close()

    return jsonify(tags)


@digital_edition.route("/<project>/occurrences/collection/<object_type>/<collection_id>")
def get_person_occurrences_by_collection(project, object_type, collection_id):
    connection = db_engine.connect()
    occurrence_sql = "SELECT publication.publication_collection_id AS collection_id, event_occurrence.id, event_occurrence.event_id, \
    type, description, event_occurrence.publication_id, event_occurrence.publication_version_id, event_occurrence.publication_facsimile_id, \
    event_occurrence.publication_comment_id, event_occurrence.publication_manuscript_id FROM event_occurrence, publication \
    WHERE event_occurrence.publication_id=publication.id AND publication.publication_collection_id={} AND \
    event_occurrence.type='{}'".format(collection_id, object_type)

    occurrences = []
    result = connection.execute(occurrence_sql)
    row = result.fetchone()
    while row is not None:
        occurrences.append(dict(row))
        row = result.fetchone()

    subjects = []
    for occurrence in occurrences:
        subject_sql = "SELECT DISTINCT event_connection.subject_id, subject.full_name, subject.legacy_id, subject.project_id \
        FROM event_occurrence, event_connection, subject \
        WHERE event_occurrence.event_id={} AND event_occurrence.event_id = event_connection.event_id AND \
        event_connection.subject_id =  subject.id".format(occurrence["event_id"])

        result = connection.execute(subject_sql)
        row = result.fetchone()
        while row is not None:
            subjects.append(dict(row))
            row = result.fetchone()
    connection.close()

    return jsonify(subjects)


@digital_edition.route("/<project>/song/<id>")
def get_publication_song(project, id):
    logger.info("Getting songs /{}/song/{}".format(project, id))
    connection = db_engine.connect()
    song_sql = "SELECT \
                ps.volume as song_volume, ps.id as song_id, ps.original_id as song_original_id, ps.name as song_name, ps.type as song_type, number as song_number, \
                variant as song_variant, landscape as song_landscape, place as song_place, recorder_firstname as song_recorder_firstname, \
                recorder_lastname as song_recorder_lastname, recorder_born_name as song_recorder_born_name, performer_firstname as song_performer_firstname,\
                performer_lastname as song_performer_lastname, performer_born_name as song_performer_born_name, \
                original_collection_location as song_original_collection_location, \
                original_collection_signature as song_original_collection_signature,\
                ps.original_publication_date as song_original_publication_date, page_number as song_page_number, subtype as song_subtype, \
                ps.note as song_note, ps.comment as song_comment, ps.lyrics as song_lyrics \
                FROM publication_song ps  "

    # Check if song is a number
    try:
        song_id = int(id)
        song_sql = song_sql + " WHERE ps.id = :song_id "
    except ValueError:
        song_id = id
        song_sql = song_sql + " WHERE ps.original_id = :song_id "

    statement = sqlalchemy.sql.text(song_sql).bindparams(song_id=song_id)
    return_data = []
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired song not found in database."}), 404
    else:
        return jsonify(dict(return_data)), 200, {"Access-Control-Allow-Origin": "*"}


@digital_edition.route("/<project>/subject/<id>")
def get_subject(project, id):
    logger.info("Getting subject /{}/subject/{}".format(project, id))
    connection = db_engine.connect()
    subject_sql = "SELECT * FROM subject "

    # Check if song is a number
    try:
        subject_id = int(id)
        subject_sql = subject_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        subject_id = id
        subject_sql = subject_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(subject_sql).bindparams(id=subject_id)
    return_data = []
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired subject not found in database."}), 404
    else:
        return jsonify(dict(return_data)), 200, {"Access-Control-Allow-Origin": "*"}


@digital_edition.route("/<project>/tag/<id>")
def get_tag(project, id):
    logger.info("Getting tag /{}/tag/{}".format(project, id))
    connection = db_engine.connect()
    tag_sql = "SELECT * FROM tag "

    # Check if song is a number
    try:
        tag_id = int(id)
        tag_sql = tag_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        tag_id = id
        tag_sql = tag_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(tag_sql).bindparams(id=tag_id)
    return_data = []
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired tag not found in database."}), 404
    else:
        return jsonify(dict(return_data)), 200, {"Access-Control-Allow-Origin": "*"}


@digital_edition.route("/<project>/location/<id>")
def get_location(project, id):
    logger.info("Getting location /{}/location/{}".format(project, id))
    connection = db_engine.connect()
    location_sql = "SELECT * FROM location "

    # Check if song is a number
    try:
        location_id = int(id)
        location_sql = location_sql + " WHERE id = :id AND deleted = 0 "
    except ValueError:
        location_id = id
        location_sql = location_sql + " WHERE legacy_id = :id AND deleted = 0 "

    statement = sqlalchemy.sql.text(location_sql).bindparams(id=location_id)
    return_data = []
    return_data = connection.execute(statement).fetchone()
    connection.close()

    if return_data is None:
        return jsonify({"msg": "Desired location not found in database."}), 404
    else:
        return jsonify(dict(return_data)), 200, {"Access-Control-Allow-Origin": "*"}

@digital_edition.route("/<project>/facsimiles/collections/<facsimile_collection_ids>")
def get_facsimile_collections(project, facsimile_collection_ids):
    logger.info("Getting facsimiles /{}/facsimiles/collections/{}".format(project, facsimile_collection_ids))
    connection = db_engine.connect()
    sql = """SELECT * FROM publication_facsimile_collection where id in :ids"""
    statement = sqlalchemy.sql.text(sql).bindparams(ids=tuple(facsimile_collection_ids.split(',')))
    return_data = []
    for row in connection.execute(statement).fetchall():
        return_data.append(dict(row))
    connection.close()
    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


@digital_edition.route("/<project>/facsimiles/<collection_id>/<number>/<zoom_level>")
def get_facsimile_file(project, collection_id, number, zoom_level):
    """
    Retrieve a single facsimile image file from project root

    Facsimile files are stored as follows: root/facsimiles/<collection_id>/<zoom_level>/<page_number>.jpg
    The collection_id these are sorted by is the publication_facsimile_collection id, stored as publication_id in the old database structure?

    However, the first page of a publication is not necessarily 1.jpg, as facsimiles often contain title pages and blank pages
    Thus, calling for facsimiles/1/1/1 may require fetching a file from root/facsimiles/1/1/5.jpg
    """
    # TODO OpenStack Swift support for ISILON file storage - config param for root 'facsimiles' path
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        connection = db_engine.connect()
        check_statement = sqlalchemy.sql.text("SELECT published FROM publication WHERE id = "
                                              "(SELECT publication_id FROM publication_facsimile WHERE publication_facsimile_collection_id=:coll_id LIMIT 1)").bindparams(coll_id=collection_id)
        row = connection.execute(check_statement).fetchone()
        if row is None:
            return jsonify({
                "msg": "Desired facsimile file not found in database."
            }), 404
        else:
            try:
                status = int(row[0])
            except Exception:
                return jsonify({
                    "msg": "Desired facsimile file not found in database."
                }), 404
            if status == 0:
                return jsonify({
                    "msg": "Desired facsimile file not found in database."
                }), 404
            elif status == 1:
                if not config["show_internally_published"]:
                    return jsonify({
                        "msg": "Desired facsimile file not found in database."
                    }), 404

        statement = sqlalchemy.sql.text("SELECT * FROM publication_facsimile_collection WHERE id=:coll_id").bindparams(coll_id=collection_id)
        row = connection.execute(statement).fetchone()
        if row is None:
            return jsonify({
                "msg": "Desired facsimile collection was not found in database!"
            }), 404
        elif row.folder_path != '' and row.folder_path is not None:
            file_path = safe_join(row.folder_path, collection_id, zoom_level, "{}.jpg".format(int(number)))
        else:
            file_path = safe_join(config["file_root"],
                                  "facsimiles",
                                  collection_id,
                                  zoom_level,
                                  "{}.jpg".format(int(number)))
        connection.close()

        output = io.BytesIO()
        try:
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            return jsonify({
                "msg": "Desired facsimile file not found."
            }), 404


@digital_edition.route("/<project>/facsimile/page/<legacy_id>/")
def get_facsimile_pages(project, legacy_id):
    logger.info("Getting facsimile page")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT * FROM facsimile_pages WHERE id=:l_id")
        statement = sql.bindparams(l_id=legacy_id)
        result = connection.execute(statement).fetchone()
        facs = dict(result)
        connection.close()

        return jsonify(facs), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get facsimile page.", status=404, content_type="text/json")


@digital_edition.route("/<project>/<facsimile_type>/page/image/<facs_id>/<facs_nr>")
def get_facsimile_page_image(project, facsimile_type, facs_id, facs_nr):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        logger.info("Getting facsimile page image")
    try:
        zoom_level = "4"
        if facsimile_type == 'facsimile':
            file_path = safe_join(config["file_root"],
                                  "facsimiles",
                                  facs_id,
                                  zoom_level,
                                  "{}.jpg".format(int(facs_nr)))
        elif facsimile_type == 'song-example':
            file_path = safe_join(config["file_root"],
                                  "song-example-images",
                                  facs_id,
                                  "{}.jpg".format(int(facs_nr)))

        output = io.BytesIO()
        try:
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        return Response("Couldn't get facsimile page.", status=404, content_type="text/json")


@digital_edition.route("/<project>/files/<folder>/<file_name>/")
def get_json_file(project, folder, file_name):
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        file_path = safe_join(config["file_root"], folder, "{}.json".format(str(file_name)))
        try:
            with open(file_path) as f:
                data = json.load(f)
            return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
        except Exception:
            return Response("File not found.", status=404, content_type="text/json")


@digital_edition.route("/<project>/song/id/<song_id>/")
def get_song_by_id(project, song_id):
    logger.info("Getting song by id")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT * FROM song WHERE id=:s_id")
        statement = sql.bindparams(s_id=song_id)
        result = connection.execute(statement).fetchone()
        connection.close()
        return jsonify(dict(result)), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get song by id.", status=404, content_type="text/json")


@digital_edition.route("/<project>/songs/filtered", methods=["GET"])
def get_songs_filtered(project):
    """
    Filter songs either by subject name, location or category.
    If no filter is provided all songs are returned.
    Sql injection is also prevented.

    Example: /<project>/songs/filtered?subject_name=Foo Bar
    """

    logger.info("Getting songs filtered...")
    try:
        connection = db_engine.connect()

        filter_query_sql = ''
        subject_name = ''
        category = ''
        location = ''

        if request.args.get('subject_name') and request.args.get('subject_name') != '':
            subject_name = request.args.get('subject_name')
            # Prevent sql injection
            filter_query_sql += 'and (s1.full_name = :s_name OR s2.full_name = :s_name )'
        elif request.args.get('category') and request.args.get('category') != '':
            category = request.args.get('category')
            # Prevent sql injection
            filter_query_sql += 'and t.name=:ca'
        elif request.args.get('location') and request.args.get('location') != '':
            location = request.args.get('location')
            # Prevent sql injection
            filter_query_sql += 'and l.city=:lo'

        song_query = "SELECT \
                        e.description as song_name, \
                        t.name as song_type, \
                        eo.publication_facsimile_page, \
                        ec1.subject_id as playman_id, \
                        s1.full_name as playman_name, \
                        ec2.subject_id as recorder_id, \
                        s2.full_name as recorder_name, \
                        t.id as tag_id, \
                        l.id as location_id, \
                        l.name as location_name, \
                        l.city as city, \
                        l.region as region, \
                        pf.publication_facsimile_collection_id, \
                        eo.publication_song_id, \
                        pf.page_nr \
                        FROM event e \
                        join event_occurrence eo \
                        on eo.event_id=e.id \
                        join event_connection ec1 \
                        on ec1.event_id = e.id \
                        join tag t \
                        on t.id = ec1.tag_id \
                        join subject s1 \
                        on ec1.subject_id = s1.id \
                        join event_connection ec2 \
                        on ec2.event_id = e.id \
                        join subject s2 \
                        on ec2.subject_id = s2.id \
                        join location l \
                        on l.id=ec1.location_id \
                        left join publication_facsimile pf \
                        on pf.id=eo.publication_facsimile_id \
                        where e.type='song' \
                        and s1.type='playman' \
                        and s2.type='recorder' \
                        {} \
                        order by e.id".format(filter_query_sql)

        sql = sqlalchemy.sql.text(song_query)

        if subject_name != '':
            # Filter songs involving person
            statement = sql.bindparams(s_name=subject_name)
        elif category != '':
            # Filter songs by category
            statement = sql.bindparams(ca=category)
        elif location != '':
            # Filter songs by location
            statement = sql.bindparams(lo=location)
        else:
            # All songs
            statement = sql

        result = connection.execute(statement).fetchall()
        return_data = []
        for row in result:
            return_data.append(dict(row))
        return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception as e:
        return Response("Couldn't get songs filtered." + str(e), status=404, content_type="text/json")


@digital_edition.route("/<project>/media/data/<type>/<type_id>")
def get_media_data(project, type, type_id):
    logger.info("Getting media data...")
    media_column = "{}_id".format(type)
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.id, media.description FROM media \
            JOIN media_connection mc ON mc.media_id = media.id \
            WHERE mc.{}=:m_id AND type='image' ".format(media_column))
        statement = sql.bindparams(m_id=type_id)
        result = connection.execute(statement).fetchone()
        result = dict(result)
        result["image_path"] = "/" + safe_join(project, "media", "image", str(result["id"]))
        connection.close()
        return jsonify(result), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get media data.", status=404, content_type="text/json")


@digital_edition.route("/<project>/media/articles/<type>/<type_id>")
def get_media_article_data(project, type, type_id):
    logger.info("Getting media data...")
    media_column = "{}_id".format(type)
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.id, media.description FROM media \
            JOIN media_connection mc ON mc.media_id = media.id \
            WHERE mc.{}=:m_id AND type = 'pdf'".format(media_column))
        statement = sql.bindparams(m_id=type_id)
        return_data = []
        for row in connection.execute(statement).fetchall():
            row = dict(row)
            row["pdf_path"] = "/" + safe_join(project, "media", "pdf", str(row["id"]))
            return_data.append(row)
        connection.close()
        return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get article data.", status=404, content_type="text/json")

@digital_edition.route("/<project>/media/image/<id>")
def get_media_data_image(project, id):
    logger.info("Getting media image...")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.image FROM media WHERE id = :image_id ").bindparams(image_id=id)
        result = connection.execute(sql).fetchone()
        result = dict(result)
        connection.close()
        return Response(io.BytesIO(result["image"]), status=200, content_type="image/jpeg")
    except Exception:
        return Response("Couldn't get media image.", status=404, content_type="text/json")

@digital_edition.route("/<project>/media/connections/<type>/<media_id>")
def get_media_connections(project, type, media_id):
    logger.info("Getting media connection data...")
    if type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()

        sql = sqlalchemy.sql.text(f"SELECT t.* FROM media_connection mcon \
            JOIN {type} t ON t.id = mcon.{type_column} \
            JOIN media m ON m.id = mcon.media_id \
            JOIN media_collection mcol ON mcol.id = m.media_collection_id \
            WHERE m.id = :id \
            AND t.project_id = :p_id \
            AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1")
        statement = sql.bindparams(id=media_id, p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/connections/<type>")
@digital_edition.route("/<project>/gallery/connections/<type>/<gallery_id>")
def get_gallery_connections(project, type, gallery_id=None):
    logger.info("Getting gallery connection data...")
    if type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get gallery connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()
        if gallery_id is not None:
            sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                            mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                        JOIN {type} t ON t.id = mcon.{type_column} \
                                        JOIN media m ON m.id = mcon.media_id \
                                        JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                        WHERE mcol.id = :id \
                                        AND t.project_id = :p_id \
                                        AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1")
            statement = sql.bindparams(id=gallery_id, p_id=project_id)
        else:
            sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                            mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                        JOIN {type} t ON t.id = mcon.{type_column} \
                                        JOIN media m ON m.id = mcon.media_id \
                                        JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                        WHERE t.project_id = :p_id \
                                        AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1")
            statement = sql.bindparams(p_id=project_id)
        results = []
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get gallery connection data.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/<type>/connections/<type_id>")
@digital_edition.route("/<project>/gallery/<type>/connections/<type_id>/<limit>")
def get_type_gallery_connections(project, type, type_id, limit=None):
    logger.info("Getting type gallery connection data...")
    if type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get gallery connection data.", status=404, content_type="text/json")
    if limit is not None:
        limit = " LIMIT 1 "
    else:
        limit = "";
    type_column = "{}_id".format(type)
    try:
        project_id = get_project_id_from_name(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text(f"SELECT t.id as t_id, m.id as media_id, m.image_filename_front as filename,\
                                        mcol.id as media_collection_id, mcol.image_path, t.* FROM media_connection mcon \
                                    JOIN {type} t ON t.id = mcon.{type_column} \
                                    JOIN media m ON m.id = mcon.media_id \
                                    JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                    WHERE t.id = :id \
                                    AND t.project_id = :p_id \
                                    AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 {limit}")
        statement = sql.bindparams(id=type_id, p_id=project_id)
      
        results = []
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get type gallery connection data.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/data/<id>/<lang>")
def get_gallery_data(project, id, lang=None):
    logger.info("Getting gallery image data")
    try:
        connection = db_engine.connect()
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text("SELECT mc.id as collection_id, m.image_filename_front AS front, m.image_filename_back AS back,\
                                    mc.image_path AS folder, (SELECT text \
                                    FROM translation_text tt \
                                    JOIN translation t ON t.id = tt.translation_id \
                                    WHERE t.id = mc.title_translation_id AND tt.language = :l) AS title, tt_desc.text AS description \
                                    FROM media m \
                                    JOIN media_collection mc ON m.media_collection_id = mc.id\
                                    JOIN translation t_desc ON t_desc.id = m.description_translation_id\
                                    JOIN translation_text tt_desc ON tt_desc.translation_id = t_desc.id AND tt_desc.language=:l\
                                    WHERE mc.project_id = :p_id \
                                    AND mc.id= :image_id\
                                    AND m.type='image_ref' ").bindparams(image_id=id, p_id=project_id, l=lang)
        results = []
        for row in connection.execute(sql).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't gallery image data.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/data/<lang>")
def get_galleries(project, lang=None):
    logger.info("Getting galleries")
    try:
        connection = db_engine.connect()
        project_id = get_project_id_from_name(project)
        sql = sqlalchemy.sql.text("SELECT mc.*, count(m.id) AS media_count, \
                                    (SELECT text \
                                    FROM translation_text tt \
                                    JOIN translation t ON t.id = tt.translation_id \
                                    WHERE t.id = mc.title_translation_id AND tt.language = :l_id) AS title \
                                    FROM media m\
                                    JOIN media_collection mc ON m.media_collection_id = mc.id\
                                    WHERE m.deleted != 1 AND mc.deleted != 1 AND mc.project_id = :p_id\
                                    GROUP BY mc.id ORDER BY mc.sort_order ASC ").bindparams(p_id=project_id, l_id=lang)
        results = []
        for row in connection.execute(sql).fetchall():
            results.append(dict(row))
        connection.close()
        return jsonify(results), 200, {"Access-Control-Allow-Origin": "*"}
    except Exception:
        return Response("Couldn't get galleries.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/get/<collection_id>/<file_name>")
def get_gallery_image(project, collection_id, file_name):
    logger.info("Getting galleries")
    try:
        project_id = get_project_id_from_name(project)
        config = get_project_config(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT image_path as image_path from media_collection WHERE project_id = :p_id AND id = :id ").bindparams(p_id=project_id, id=collection_id)
        result = connection.execute(sql).fetchone()
        result = dict(result)
        connection.close()
        file_path = safe_join(config["file_root"], "media", str(result['image_path']),"{}".format(str(file_name)))
        try:
            output = io.BytesIO()
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        return Response("Couldn't get gallery file.", status=404, content_type="text/json")

@digital_edition.route("/<project>/gallery/thumb/<type>/<id>")
def get_type_gallery_image(project, type, id):
    logger.info("Getting gallery file")
    if type not in ['tag', 'location', 'subject']:
        return Response("Couldn't get media connection data.", status=404, content_type="text/json")
    type_column = "{}_id".format(type)
    try:
        project_id = get_project_id_from_name(project)
        config = get_project_config(project)
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text(f"SELECT mcol.image_path, m.image_filename_front FROM media_connection mcon \
                                    JOIN {type} t ON t.id = mcon.{type_column} \
                                    JOIN media m ON m.id = mcon.media_id \
                                    JOIN media_collection mcol ON mcol.id = m.media_collection_id \
                                    WHERE t.id = :id \
                                    AND t.project_id = :p_id \
                                    AND mcol.deleted != 1 AND t.deleted != 1 AND m.deleted != 1 AND mcon.deleted != 1 LIMIT 1").bindparams(p_id=project_id, id=id)
        result = connection.execute(sql).fetchone()
        result = dict(result)
        connection.close()
        file_path = safe_join(config["file_root"], "media", str(result['image_path']), str(result['image_filename_front']).replace(".jpg", "_thumb.jpg"))
        try:
            output = io.BytesIO()
            with open(file_path, mode="rb") as img_file:
                output.write(img_file.read())
            content = output.getvalue()
            output.close()
            return Response(content, status=200, content_type="image/jpeg")
        except Exception:
            return Response("File not found: " + file_path, status=404, content_type="text/json")
    except Exception:
        return Response("Couldn't get type file.", status=404, content_type="text/json")

# TODO: get subjects, locations and tags for gallery 

@digital_edition.route("/<project>/media/pdf/<id>")
def get_media_data_pdf(project, id):
    logger.info("Getting media image...")
    try:
        connection = db_engine.connect()
        sql = sqlalchemy.sql.text("SELECT media.pdf FROM media WHERE id = :pdf_id").bindparams(pdf_id=id)
        result = connection.execute(sql).fetchone()
        result = dict(result)
        connection.close()
        return Response(io.BytesIO(result["pdf"]), status=200, content_type="application/pdf")
    except Exception:
        return Response("Couldn't get media image.", status=404, content_type="text/json")


@digital_edition.route("/<project>/files/<collection_id>/<file_type>/<download_name>/", defaults={'use_download_name': None})
@digital_edition.route("/<project>/files/<collection_id>/<file_type>/<download_name>/<use_download_name>")
def get_pdf_file(project, collection_id, file_type, download_name, use_download_name):
    """
    Retrieve a single file from project root
    Currently only PDF or ePub
    """
    # TODO published status for facsimile table to check against?
    # TODO S3 support
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    connection = db_engine.connect()
    # Check that the collection exists
    statement = sqlalchemy.sql.text("SELECT * FROM publication_collection WHERE id=:coll_id").bindparams(coll_id=collection_id)
    row = connection.execute(statement).fetchone()
    if row is None:
        return jsonify({
            "msg": "Desired facsimile collection was not found in database!"
        }), 404

    file_path = ""

    direct_download_name = ""

    if use_download_name and 'pdf' in str(file_type):
        if '.pdf' in str(download_name):
            direct_download_name = download_name.split('.pdf')[0]
        else:
            direct_download_name = download_name

        file_path = safe_join(config["file_root"],
                              "downloads",
                              collection_id,
                              "{}.pdf".format(direct_download_name))
    elif 'pdf' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "downloads",
                              collection_id,
                              "{}.pdf".format(int(collection_id)))
    elif 'epub' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "downloads",
                              collection_id,
                              "{}.epub".format(int(collection_id)))
    connection.close()

    try:
        return send_file(file_path, attachment_filename=download_name, conditional=True)
    except Exception:
        return Response("File not found.", status=404, content_type="text/json")


@digital_edition.route("/<project>/song-files/<file_type>/<file_name>/")
def get_song_file(project, file_type, file_name):
    """
    Retrieve a single file from project root that belongs to a song
    It can be musicxml, midi
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    file_path = ""
    if 'musicxml' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "musicxml",
                              "{}.xml".format(str(file_name)))
        file_name = "{}.xml".format(str(file_name))
    elif 'midi' in str(file_type):
        file_path = safe_join(config["file_root"],
                              "midi-files",
                              "{}.mid".format(str(file_name)))

    try:
        return send_file(file_path, as_attachment=True, mimetype='application/octet-stream',
                         attachment_filename=file_name)
    except Exception:
        return Response("File not found.", status=404, content_type="text/json")


@digital_edition.route("/<project>/urn/<url>/")
@digital_edition.route("/<project>/urn/<url>/<legacy_id>/")
def get_urn(project, url, legacy_id=None):
    url = urllib.parse.unquote(urllib.parse.unquote(url))
    logger.info("Getting urn /{}/urn/{}/{}/".format(project, url, legacy_id))
    project_id = get_project_id_from_name(project)
    connection = db_engine.connect()
    if legacy_id is not None:
        sql = sqlalchemy.sql.text("SELECT * FROM urn_lookup where legacy_id=:l_id  AND project_id=:p_id").bindparams(l_id=str(legacy_id), p_id=project_id)
    else:
        url_like_str = "%#{}".format(url)
        sql = sqlalchemy.sql.text("SELECT * FROM urn_lookup where url LIKE :url AND project_id=:p_id").bindparams(url=url_like_str, p_id=project_id)
    return_data = []
    for row in connection.execute(sql).fetchall():
        return_data.append(dict(row))
    connection.close()
    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


def list_tooltips(table):
    """
    List available tooltips for subjects, tags, or locations
    table should be 'subject', 'tag', or 'location'
    """
    if table not in ["subject", "tag", "location"]:
        return ""
    connection = db_engine.connect()
    if table == "subject":
        sql = sqlalchemy.sql.text("SELECT id, full_name, project_id, legacy_id FROM subject")
    else:
        sql = sqlalchemy.sql.text("SELECT id, name, project_id, legacy_id FROM {}".format(table))
    results = []
    for row in connection.execute(sql).fetchall():
        results.append(dict(row))
    connection.close()
    return results


def get_tooltip(table, row_id, project=None, use_legacy=False):
    """
    Get 'tooltip' style info for a single subject, tag, or location by its ID
    table should be 'subject', 'tag', or 'location'
    """
    connection = db_engine.connect()
    try:
        ident = int(row_id)
        is_legacy_id = False
    except ValueError:
        ident = str(row_id)
        is_legacy_id = True

    if use_legacy:
        ident = str(row_id)
        is_legacy_id = True

    project_sql = " AND project_id = :project_id "
    if project is None:
        project_sql = ""

    if is_legacy_id:
        if table == "subject":
            sql = sqlalchemy.sql.text(
                "SELECT id, legacy_id, full_name, description FROM subject WHERE legacy_id=:id" + project_sql)
        else:
            sql_query = "SELECT id, legacy_id, name, description FROM {} WHERE legacy_id=:id " + project_sql
            sql = sqlalchemy.sql.text(sql_query.format(table))
    else:
        if table == "subject":
            sql = sqlalchemy.sql.text(
                "SELECT id, legacy_id, full_name, description FROM subject WHERE id=:id" + project_sql)
        else:
            sql_query = "SELECT id, legacy_id, name, description FROM {} WHERE id=:id" + project_sql
            sql = sqlalchemy.sql.text(sql_query.format(table))

    if project is None:
        statement = sql.bindparams(id=ident)
    else:
        project_id = get_project_id_from_name(project)
        statement = sql.bindparams(id=ident, project_id=project_id)

    result = connection.execute(statement).fetchone()
    connection.close()
    if result is None:
        return dict()
    else:
        return dict(result)


# Freetext seach through ElasticSearch API
@digital_edition.route("<project>/search/freetext/<search_text>/<fuzziness>")
def get_freetext_search(project, search_text, fuzziness=1):
    logger.info("Getting results from elastic")
    if len(search_text) > 0:
        res = es.search(index=str(project), body={
            "query":
            {
                "match":
                {
                    "textData":
                    {
                        "query": search_text,
                        "fuzziness": fuzziness
                    }
                }
            },
            "highlight": {
                "fields" : {
                    "textData" : {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Location seach through ElasticSearch API
@digital_edition.route("<project>/search/location/<search_text>/")
def get_location_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='location', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"city": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"country": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "city": {},
                    "country": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Subject seach through ElasticSearch API
@digital_edition.route("<project>/search/subject/<search_text>/")
def get_subject_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='subject', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"first_name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"last_name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"full_name": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "first_name": {},
                    "last_name": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Tag seach through ElasticSearch API
@digital_edition.route("<project>/search/tag/<search_text>/")
def get_tag_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='tag', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "name": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Tag seach through ElasticSearch API
@digital_edition.route("<project>/search/user_defined/<index>/<field>/<search_text>/<fuzziness>/")
def get_user_defined_search(project, index, field, search_text, fuzziness):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index=str(index), body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {str(field): {"query": str(search_text), "fuzziness": int(fuzziness)}}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    str(field): {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


@digital_edition.route("/<project>/search/suggestions/<search_string>/<limit>")
def get_search_suggestions(project, search_string, limit):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_string) > 0:
        res = es.search(index="tag,location,subject,song," + str(project), body={
            "size": limit,
            "indices_boost" : [
                { "song" : 2.0 },
                { "subject" : 2.0 },
                { "location" : 2.0 },
                { "tag" : 2.0 }
            ],
            "_source": {
                "includes": [""]
            },
            "query" : {
                "bool": {
                "should": [
                    {
                    "bool": {
                        "must" : [
                            {
                                "multi_match" : {
                                    "query" : str(search_string),
                                    "type" : "phrase_prefix",
                                    "fields" : [ "*" ],
                                    "lenient" : True
                                }
                            }
                        ,
                        {
                            "match": {
                                "project_id": str(project_id)
                            }
                        }
                        ]
                    }
                    },
                    {
                    "bool": {
                        "must" : [
                            {
                            "multi_match" : {
                                "query" : str(search_string),
                                "type" : "phrase_prefix",
                                "fields" : [ "*" ],
                                "lenient" : True
                            }
                            }
                        ,
                        {
                            "match": {
                                "_index": str(project)
                            }
                        }
                        ]
                    }
                    }
                ]
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "full_name": {},
                    "song_name": {},
                    "message": {},
                    "textData": {}
                },
                "boundary_scanner": "word",
                "number_of_fragments": 1
            }
            })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")

@digital_edition.route("/<project>/search/all/<search_string>/<limit>")
def get_search_all(project, search_string, limit):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_string) > 0:
        res = es.search(index="tag,location,subject,song," + str(project), body={
            "size": limit,
            "indices_boost" : [
                { "song" : 2.0 },
                { "subject" : 2.0 },
                { "location" : 2.0 },
                { "tag" : 2.0 }
            ],
            "query" : {
                "bool": {
                "should": [
                    {
                    "bool": {
                        "must" : [
                            {
                                "multi_match" : {
                                    "query" : str(search_string),
                                    "type" : "phrase_prefix",
                                    "fields" : [ "*" ],
                                    "lenient" : True
                                }
                            }
                        ,
                        {
                            "match": {
                                "project_id": str(project_id)
                            }
                        }
                        ]
                    }
                    },
                    {
                    "bool": {
                        "must" : [
                            {
                            "multi_match" : {
                                "query" : str(search_string),
                                "type" : "phrase_prefix",
                                "fields" : [ "*" ],
                                "lenient" : True
                            }
                            }
                        ,
                        {
                            "match": {
                                "_index": str(project)
                            }
                        }
                        ]
                    }
                    }
                ]
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "full_name": {},
                    "song_name": {},
                    "message": {},
                    "textData": {}
                },
                "boundary_scanner": "sentence",
                "number_of_fragments": 1,
                "boundary_max_scan": 10
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")
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
    path = '-'.join(path[i:i + 2] for i in range(0, len(path), 2))
    return path


def slugify_path(project, path):
    config = get_project_config(project)
    path = split_after(path, "/" + config["file_root"] + "/md/")
    return re.sub('.md', '', path)


def path_hierarchy(project, path, language):
    config = get_project_config(project)
    hierarchy = {'id': slugify_id(path, language), 'title': filter_title(os.path.basename(path)),
                 'basename': re.sub('.md', '', os.path.basename(path)), 'path': slugify_path(project, path),
                 'fullpath': path,
                 'route': slugify_route(split_after(path, "/" + config["file_root"] + "/md/")),
                 'type': 'folder',
                 'children': [path_hierarchy(project, p, language) for p in sorted(glob.glob(os.path.join(path, '*')))]}

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
    Returns info on if project, publication_collection, and publication are all published
    Returns two values:
        - a boolean if the publication can be shown
        - a message text why it can't be shown, if that is the case

    Publications can be shown if they're externally published (published==2),
    or if they're internally published (published==1) and show_internally_published is True
    """
    config = get_project_config(project)
    if config is None:
        return False, "No such project."
    connection = db_engine.connect()
    select = """SELECT project.published AS proj_pub, publication_collection.published AS col_pub, publication.published as pub
    FROM project
    JOIN publication_collection ON publication_collection.project_id = project.id
    JOIN publication ON publication.publication_collection_id = publication_collection.id
    WHERE project.id = publication_collection.project_id
    AND publication.publication_collection_id = publication_collection.id
    AND project.name = :project AND publication_collection.id = :c_id AND (publication.id = :p_id OR split_part(publication.legacy_id, '_', 2) = :str_p_id)
    """
    statement = sqlalchemy.sql.text(select).bindparams(project=project, c_id=collection_id, p_id=publication_id,
                                                       str_p_id=str(publication_id))
    result = connection.execute(statement)
    show_internal = config["show_internally_published"]
    can_show = False
    message = ""
    row = result.fetchone()
    if row is None:
        message = "Content does not exist"
    else:
        if row.proj_pub is None or row.col_pub is None or row.pub is None:
            status = -1
        else:
            status = min(row.proj_pub, row.col_pub, row.pub)
        if status < 1:
            message = "Content is not published"
        elif status == 1 and not show_internal:
            message = "Content is not externally published"
        else:
            can_show = True
    connection.close()
    return can_show, message


def get_title_published_status(project, collection_id):
    """
    Returns info on if project, publication_collection, and publication are all published
    Returns two values:
        - a boolean if the publication can be shown
        - a message text why it can't be shown, if that is the case

    Publications can be shown if they're externally published (published==2),
    or if they're internally published (published==1) and show_internally_published is True
    """
    config = get_project_config(project)
    if config is None:
        return False, "No such project."
    connection = db_engine.connect()

    project_id = get_project_id_from_name(project)

    select = """SELECT project.published AS proj_pub, publication_collection.published AS col_pub, publication_collection_title.published as pub
    FROM project
    JOIN publication_collection ON publication_collection.project_id = project.id
    JOIN publication_collection_title ON publication_collection_title.id = publication_collection.publication_collection_title_id
    AND project.id = :project_id AND publication_collection.id = :c_id
    """
    statement = sqlalchemy.sql.text(select).bindparams(project_id=project_id, c_id=collection_id)
    result = connection.execute(statement)
    show_internal = config["show_internally_published"]
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
    connection.close()
    return can_show, message


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def xml_to_html(xsl_file_path, xml_file_path, replace_namespace=False, params=None):
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
            xml_contents = xml_contents.replace(b'xmlns="http://www.sls.fi/tei"',
                                                b'xmlns="http://www.tei-c.org/ns/1.0"')

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
        raise Exception(
            "Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {}".format(
                type(params)))
    if len(xsl_transform.error_log) > 0:
        logging.debug(xsl_transform.error_log)
    return str(result)


def get_content(project, folder, xml_filename, xsl_filename, parameters):
    config = get_project_config(project)
    if config is None:
        return "No such project."
    xml_file_path = safe_join(config["file_root"], "xml", folder, xml_filename)
    xsl_file_path = safe_join(config["file_root"], "xslt", xsl_filename)
    cache_folder = os.path.join("/tmp", "api_cache", project, folder)
    os.makedirs(cache_folder, exist_ok=True)
    if "ms" in xsl_filename:
        # xsl_filename is 'ms_changes.xsl' or 'ms_normalized.xsl'
        # ensure that '_changes' or '_normalized' is appended to the cache filename accordingly
        cache_extension = "{}.html".format(xsl_filename.split("ms")[1].replace(".xsl", ""))
    else:
        cache_extension = ".html"
    cache_file_path = os.path.join(cache_folder, xml_filename.replace(".xml", cache_extension))

    content = None
    param_ext = ''
    if parameters is not None:
        if 'noteId' in parameters:
            param_ext += "_" + parameters["noteId"]
        if 'sectionId' in parameters:
            param_ext += "_" + parameters["sectionId"]
        # not needed for bookId
        param_file_name = xml_filename.split(".xml")[0] + param_ext
        cache_file_path = cache_file_path.replace(xml_filename.split(".xml")[0], param_file_name)
        cache_file_path = cache_file_path.replace('"', '')

    logger.debug("Cache file path for {} is {}".format(xml_filename, cache_file_path))

    if os.path.exists(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                logger.exception("Error reading content from cache for {}".format(cache_file_path))
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
