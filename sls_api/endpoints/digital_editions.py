from collections import OrderedDict
from flask import abort, Blueprint, jsonify, safe_join
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import pymysql
from ruamel.yaml import YAML
import os
from sys import stdout

# TODO cache invalidation?

digital_edition = Blueprint('digital_edition', __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as digital_editions_config:
    yaml = YAML()
    project_config = yaml.load(digital_editions_config)

logger = logging.getLogger("digital_editions_api")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(stream=stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(stream_handler)

file_handler = TimedRotatingFileHandler(filename=project_config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)


def xml_to_html(xsl_file_path, xml_file_path, replace_namespace=True, params=None):
    if not os.path.exists(xsl_file_path):
        return "XSL file {!r} not found!".format(xsl_file_path)
    if not os.path.exists(xml_file_path):
        return "XML file {!r} not found!".format(xml_file_path)

    with open(xml_file_path, encoding="UTF-8") as xml_file:
        xml_contents = xml_file.read()
        if replace_namespace:
            xml_contents = xml_contents.replace('xmlns="http://www.sls.fi/tei"', 'xmlns="http://www.tei-c.org/ns/1.0"')

        xml_root = etree.fromstring(xml_contents)

    with open(xsl_file_path, encoding="UTF-8") as xsl_file:
        xslt_root = etree.parse(xsl_file)
        xsl_transform = etree.XSLT(xslt_root)

    if params:
        result = xsl_transform(xml_root, **params)
    else:
        result = xsl_transform(xml_root)

    return str(result)


def open_mysql_connection(database):
    class OrderedDictCursor(pymysql.cursors.DictCursorMixin, pymysql.cursors.Cursor):
        dict_type = OrderedDict
    global connection
    if database not in project_config:
        connection = None
    else:
        connection = pymysql.connect(
            host=project_config[database]["address"],
            user=project_config[database]["username"],
            password=project_config[database]["password"],
            db=project_config[database]["database"],
            charset="utf8",
            cursorclass=OrderedDictCursor
        )


@digital_edition.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response


# routes/digitaledition/html.php
@digital_edition.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    logger.info("Getting static content from /{}/html/{}".format(project, filename))
    file_path = safe_join(project_config[project]["file_root"], "html", "{}.html".format(filename))
    # file_path = os.path.join(os.path.abspath(__file__), os.path.realpath("/../../../../{}-required/html/".format(project)), "{}.html".format(filename))
    if os.path.exists(file_path):
        with open(file_path, encoding="UTF-8") as html_file:
            contents = html_file.read()
        data = {
            "filename": filename,
            "contents": contents
        }
        return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
    else:
        abort(404)


# routes/digitaledition/manuscripts.php
@digital_edition.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    open_mysql_connection(project)
    sql = "SELECT * FROM manuscripts WHERE m_publication_id=%s ORDER BY m_sort"
    with connection.cursor() as cursor:
        cursor.execute(sql, [publication_id])
        results = cursor.fetchall()
    connection.close()
    return jsonify(results)


# routes/digitaledition/publications.php
@digital_edition.route("/<project>/publication/<publication_id>")
def get_publication(project, publication_id):
    logger.info("Getting publication /{}/publication/{}".format(project, publication_id))
    open_mysql_connection(project)
    sql = "SELECT * FROM publications WHERE p_id=%s ORDER BY p_title"
    with connection.cursor() as cursor:
        cursor.execute(sql, [publication_id])
        results = cursor.fetchall()
    connection.close()
    return jsonify(results)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/editions")
def get_toc_editions(project):
    logger.info("Getting editions /{}/table-of-contents/editions".format(project))
    open_mysql_connection(project)
    if project_config.get(project).get("show_unpublished"):
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed ORDER BY ed_datumlansering"
    elif project_config.get(project).get("show_internally_published"):
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed WHERE ed_lansering>0 ORDER BY ed_datumlansering"
    else:
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed WHERE ed_lansering=2 ORDER BY ed_datumlansering"

    with connection.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()

    connection.close()
    return_data = []
    for row in result:
        if row["id"] not in project_config.get(project).get("disabled_publications"):
            return_data.append(row)

    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/root")
def get_toc_root(project, edition_id):
    logger.info("Getting root s/{}/table-of-contents/{}/root".format(project, edition_id))
    open_mysql_connection(project)
    show_published = 2
    if project_config.get(project).get("show_unpublished"):
        show_published = 0
    elif project_config.get(project).get("show_internally_published"):
        show_published = 1

    if edition_id == 15:
        sql = "SELECT toc.* FROM tableofcontents toc " \
              "JOIN publications_ed ped ON toc.toc_ed_id = ped.ed_id " \
              "JOIN publications_group pgroup ON toc.toc_group_id = pgroup.group_id " \
              "WHERE toc_ed_id=%s AND pgroup.group_lansering>={} AND toc_groupid IS NULL ORDER BY sortOrder".format(show_published)
    else:
        sql = "SELECT toc.* FROM tableofcontents toc " \
              "WHERE toc_ed_id=%s AND toc_group_id IS NULL AND toc_groupid IS NULL ORDER BY sortOrder"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        results = cursor.fetchall()
    connection.close()

    return jsonify(results)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/group/<group_id>")
def get_toc_root_elements(project, edition_id, group_id):
    logger.info("Getting \"root elements\" /{}/table-of-contents/edition/{}/group/{}".format(project, edition_id, group_id))
    open_mysql_connection(project)
    sql = "SELECT * FROM tableofcontents WHERE toc_ed_id=%s AND toc_groupid=%s AND toc_linkType!=6 ORDER BY sortOrder"
    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id, group_id])
        results = cursor.fetchall()
    connection.close()
    return jsonify(results)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/prevnext/<link_id>")
def get_toc_edition_link(project, edition_id, link_id):
    logger.info("Getting links /{}/table-of-contents/edition/{}/prevnext/{}".format(project, edition_id, link_id))
    return_data = []
    open_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, " \
          "ed_filediv AS multiple_files, ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id=%s ORDER BY ed_datumlansering"
    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        edition_data = cursor.fetchall()

    if edition_data[0]["multiple_files"] == "0":
        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id FROM tableofcontents t1 " \
              "LEFT JOIN tableofcontents t2 ON t1.toc_linkType=t2.toc_linkType AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=%s AND t2.toc_linkID=%s AND t1.toc_id < t2.toc_id ORDER BY t1.sortorder DESC LIMIT 1"
        with connection.cursor() as cursor:
            cursor.execute(sql, [edition_id, link_id])
            toc_data = cursor.fetchall()
            return_data["prev"] = toc_data[0]

        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=%s AND t2.toc_linkID=%s " \
              "AND t1.toc_id > t2.toc_id ORDER BY t1.sortorder ASC LIMIT 1"
        with connection.cursor() as cursor:
            cursor.execute(sql, [edition_id, link_id])
            toc_data = cursor.fetchall()
            return_data["next"] = toc_data[0]
    else:
        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_groupid=t2.toc_groupid " \
              "AND t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=%s AND t2.toc_linkID=%s " \
              "AND t1.toc_id < t2.toc_id ORDER BY t1.sortorder DESC LIMIT 1"
        with connection.cursor() as cursor:
            cursor.execute(sql, [edition_id, link_id])
            toc_data = cursor.fetchall()
            return_data["prev"] = toc_data[0]

        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_groupid=t2.toc_groupid " \
              "AND t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=%s AND t2.toc_linkID=%s " \
              "AND t1.toc_id > t2.toc_id ORDER BY t1.sortorder ASC LIMIT 1"
        with connection.cursor() as cursor:
            cursor.execute(sql, [edition_id, link_id])
            toc_data = cursor.fetchall()
            return_data["next"] = toc_data[0]

    connection.close()
    return jsonify(return_data)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>")
def get_toc_edition(project, edition_id):
    logger.info("Getting edition /{}/table-of-contents/edition/{}".format(project, edition_id))
    open_mysql_connection(project)

    show_published = 2
    if project_config.get(project).get("show_unpublished"):
        show_published = 0
    elif project_config.get(project).get("show_internally_published"):
        show_published = 1

    if edition_id == 15:
        sql = "SELECT toc.* FROM tableofcontents toc JOIN publications_ed ped ON toc.toc_ed_id = ped.ed_id " \
              "JOIN publications_group pgroup ON toc.toc_group_id = pgroup.group_id " \
              "WHERE toc_ed_id = %s AND toc_linkType!=6 AND prgroup.group_lansering>={} " \
              "ORDER BY toc_groupid, toc.sortOrder".format(show_published)
    else:
        sql = "SELECT toc.* FROM tableofcontents toc WHERE toc_ed_id=%s AND toc_linkType!=6 " \
              "AND toc.toc_group_id IS NULL ORDER BY toc_groupid, toc.sortOrder"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        toc_data = cursor.fetchall()
    connection.close()

    return_data = OrderedDict()
    sub_group_toc_id = 0

    for iteration in range(1, 3):
        for key, value in toc_data.items():
            if iteration == 1:
                if value["toc_groupid"] is None:
                    if value["toc_id"] not in return_data:
                        return_data[value["toc_id"]] = {
                            "id": value["toc_id"],
                            "title": value["title"],
                            "titleLevel": value["titleLevel"],
                            "items": []
                        }
            elif iteration == 2:
                if value["toc_groupid"] is not None and value["toc_linkType"] is None:
                    if value["toc_groupid"] not in return_data or "items" not in return_data[value["toc_groupid"]] or value["toc_id"] not in return_data[value["toc_groupid"]]["items"]:
                        sub_group_toc_id = value["toc_id"]
                        return_data[value["toc_groupid"]]["items"][value["toc_id"]] = {
                            "title": value["title"],
                            "id": value["toc_id"],
                            "items": []
                        }
                    else:
                        return_data[value["toc_groupid"]]["items"][sub_group_toc_id]["items"][value["toc_id"]] = {
                            "title": value["title"],
                            "id": value["toc_id"],
                            "link": "{}_{}".format(value["toc_ed_id"], value["toc_linkID"]),
                            "link_type": value["toc_linkType"],
                            "sort_order": value["sortOrder"]
                        }

    # TODO attempt to make sense of line 176 - 212 in https://github.com/slsfi/digital_editions_API/blob/master/src/routes/digitaledition/table-of-contents.php
    return_data = _php_array_values(return_data)

    for key, value in return_data.items():
        return_data[key]["items"] = _php_array_values(return_data[key]["items"])

        if len(return_data[key]["items"]) == 0:
            for key2, value2 in return_data[key]["items"].items():
                return_data[key]["items"][key2]["items"] = _php_array_values(return_data[key]["items"][key2]["items"])

    return jsonify(return_data)


def _php_array_values(ordered_dict):
    i = 0
    return_dict = OrderedDict()
    for key, value in ordered_dict:
        return_dict[i] = value
        i += 1
    return return_dict


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/first")
def get_toc_edition_firstentry(project, edition_id):
    logger.info("Getting first edition /{}/table-of-contents/edition/{}/first".format(project, edition_id))
    open_mysql_connection(project)
    sql = "SELECT title, toc_ed_id, toc_linkID FROM tableofcontents WHERE toc_ed_id=%s AND toc_linkID IS NOT NULL ORDER BY sortOrder ASC LIMIT 1"
    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        result = cursor.fetchone()
    connection.close()
    return jsonify(result)


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/est/<edition_id>")
def get_publication_est_text(project, edition_id):
    logger.info("Getting XML /{}/text/est/{} and transforming".format(project, edition_id))
    open_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, ed_filediv AS multiple_files, " \
          "ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id = %s ORDER BY ed_datumlansering"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id.split("_")[0]])  # edition_id is like 1_1, we need the first digit here
        edition_data = cursor.fetchall()

    sql = "SELECT DISTINCT p.p_identifier FROM digital_edition_topelius.publications_ed ped " \
          "JOIN digital_edition_topelius.publications p ON p.p_ed_id = ped.ed_id " \
          "JOIN digital_edition_topelius.publications_collection pc ON pc.coll_ed_id = p.p_ed_id " \
          "JOIN digital_edition_topelius.publications_group pg ON pg.group_id = p.p_group_id " \
          "WHERE ped.ed_lansering = 2 AND pg.group_lansering != 1 AND ped.ed_id = 15 AND p.p_identifier = %s"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        letters = cursor.fetchall()
        letter_published = True

    connection.close()

    if len(letters) > 0:
        letter_published = False

    if len(edition_data) < 1:
        content = "Content does not exist"
    elif edition_data[0]["ed_lansering"] < 1 and not project_config[project]["show_unpublished"]:
        content = "Content is not published"
    elif edition_data[0]["ed_lansering"] == 1 and not project_config[project]["show_internally_published"] and not project_config[project]["show_unpublished"]:
        content = "Content is not externally published"
    else:
        id_parts = edition_id.replace("_est", "").split(";")

        if not project_config[project]["show_internally_published"] and not letter_published:
            content = "Content is not externally published"

        else:
            xml_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))

            cache_file_path = safe_join(project_config[project]["file_root"], "cache", "est", "{}_est.html".format(id_parts[0]))

            logger.debug("Cache file path is {}".format(cache_file_path))
            logger.debug("XML file path is {}".format(xml_file_path))

            if os.path.exists(cache_file_path):
                try:
                    with open(cache_file_path, encoding="UTF-8") as cache_file:
                        content = cache_file.read()
                except Exception:
                    content = "Error reading file from cache."

            elif os.path.exists(xml_file_path):
                logger.warning("No cache found")
                try:
                    xsl_file_path = safe_join(project_config["xslt_root"], "est.xsl")
                    content = xml_to_html(xsl_file_path, xml_file_path)
                    try:
                        with open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                            logger.warning("Writing contents to cache file")
                            cache_file.write(content)
                    except Exception:
                        logger.exception("Could not create cachefile")
                        content = "Successfully fetched content but could not generate cache for it."
                except Exception:
                    logger.exception("Error when parsing XML file")
                    content = "Error parsing document."
            else:
                content = "File not found."

    data = {
        "id": edition_id,
        "content": content.replace("id=", "data-id=")
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/com/<edition_id>/<note_id>")
def get_publication_com_text(project, edition_id, note_id):
    logger.info("Getting XML /{}/text/com/{}/{} and transforming".format(project, edition_id, note_id))
    open_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, ed_filediv AS multiple_files, " \
          "ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id = %s ORDER BY ed_datumlansering"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id.split("_")[0]])  # edition_id is like 1_1, we need the first digit here
        edition_data = cursor.fetchall()

    sql = "SELECT DISTINCT p.p_identifier FROM digital_edition_topelius.publications_ed ped " \
          "JOIN digital_edition_topelius.publications p ON p.p_ed_id = ped.ed_id " \
          "JOIN digital_edition_topelius.publications_collection pc ON pc.coll_ed_id = p.p_ed_id " \
          "JOIN digital_edition_topelius.publications_group pg ON pg.group_id = p.p_group_id " \
          "WHERE ped.ed_lansering = 2 AND pg.group_lansering != 1 AND ped.ed_id = 15 AND p.p_identifier = %s"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        letters = cursor.fetchall()
        letter_published = False

    connection.close()

    if len(letters) > 0 or not edition_id.startswith("15_"):
        letter_published = True

    if len(edition_data) < 1:
        content = "Content does not exist"
    elif edition_data[0]["ed_lansering"] < 1 and not project_config[project]["show_unpublished"]:
        content = "Content is not published"
    elif edition_data[0]["ed_lansering"] == 1 and not project_config[project]["show_internally_published"] and not project_config[project]["show_unpublished"]:
        content = "Content is not externally published"
    else:
        if not project_config[project]["show_internally_published"] and not letter_published:
            content = "Content is not externally published"
        else:
            id_parts = edition_id.replace("_com", "").split(";")

            xml_file_path = safe_join(project_config[project]["file_root"], "xml", "com", "{}_com.xml".format(id_parts[0]))
            # xml_file_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/xml/com".format(project), "{}_com.xml".format(id_parts[0])))
            est_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))
            # est_file_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/xml/est".format(project), "{}_est.xml".format(id_parts[0])))

            cache_file_path = safe_join(project_config[project]["file_root"], "cache", "com", "note_{}_com_{}.html".format(id_parts[0], note_id))
            # cache_folder_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/cache/com".format(project)))
            # cache_file_path = os.path.join(cache_folder_path, "note_{}_com_{}.html".format(id_parts[0], note_id))

            logger.debug("Cache file path is {}".format(cache_file_path))
            logger.debug("XML file path is {}".format(xml_file_path))
            logger.debug("est XML file path is {}".format(est_file_path))

            if os.path.exists(cache_file_path):
                try:
                    with open(cache_file_path, encoding="UTF-8") as cache_file:
                        content = cache_file.read()
                except Exception:
                    content = "Error reading content from cache."
            elif os.path.exists(xml_file_path):
                logger.warning("No cache found")
                try:
                    params = {
                        "noteId": note_id,
                        "estDocument": "file://{}".format(est_file_path)
                    }
                    xsl_file_path = safe_join(project_config["xslt_root"], "notes.xsl")

                    content = xml_to_html(xsl_file_path, xml_file_path, params=params)
                    try:
                        with open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                            logger.warning("Writing contents to cache file")
                            cache_file.write(content)
                    except Exception:
                        logger.exception("Could not create cachefile")
                        content = "Successfully fetched content but could not generate cache for it."
                except Exception:
                    logger.exception("Error when parsing XML file")
                    content = "Error parsing document"
            else:
                content = "File not found"

    data = {
        "id": edition_id,
        "content": content
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/inl/<edition_id>")
@digital_edition.route("/<project>/text/inl/<edition_id>/<lang>")
def get_publication_inl_text(project, edition_id, lang=None):
    if lang is None:
        logger.info("Getting XML /{}/text/inl/{} and transforming".format(project, edition_id))
    else:
        logger.info("Getting XML /{}/text/inl/{}/{} and transforming".format(project, edition_id, lang))
    open_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, ed_filediv AS multiple_files, " \
          "ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id = %s ORDER BY ed_datumlansering"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id.split("_")[0]])  # edition_id is like 1_1, we need the first digit here
        edition_data = cursor.fetchall()

    if len(edition_data) < 1:
        content = "Content does not exist"
    elif edition_data[0]["ed_lansering"] < 1 and not project_config[project]["show_unpublished"]:
        content = "Content is not published"
    elif edition_data[0]["ed_lansering"] == 1 and not project_config[project]["show_internally_published"] and not project_config[project]["show_unpublished"]:
        content = "Content is not externally published"
    else:
        lang_code = "fin" if lang == "fi" else "swe"
        version = "int" if project_config[project]["show_internally_published"] else "ext"
        filename = "{}_inl_{}_{}.xml".format(edition_id, lang_code, version)

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "inl", filename)
        # xml_file_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/xml/inl".format(project), filename))

        cache_file_path = safe_join(project_config[project]["file_root"], "cache", "inl", filename.replace(".xml", ".html"))
        # cache_folder_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/cache/inl".format(project)))
        # cache_file_path = os.path.join(cache_folder_path, filename.replace(".xml", ".html"))

        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))

        if os.path.exists(cache_file_path):
            logger.info("Cache file found, reading contents from there...")
            try:
                with open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                logger.exception("Error reading content from cache")
                content = "Error reading content from cache"
        elif os.path.exists(xml_file_path):
            logger.warning("No cache found")
            try:
                xsl_file_path = safe_join(project_config["xslt_root"], "est.xml")
                content = xml_to_html(xsl_file_path, xml_file_path)
                try:
                    with open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.warning("Writing contents to cache file")
                        cache_file.write(content)
                except Exception:
                    logger.exception("Could not create cachefile")
                    content = "Successfully fetched content but could not generate cache for it."
            except Exception:
                logger.exception("Error parsing document")
                content = "Error parsing document"
        else:
            logger.warning("No preface found")
            content = "File not found"

    data = {
        "id": edition_id,
        "content": content
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/semantic_data/persons.php
@digital_edition.route("/semantic_data/persons/tooltip/<person_id>")
def get_person_tooltip(person_id):
    logger.info("Getting tooltip /semantic_data/persons/tooltip/{}".format(person_id))
    open_mysql_connection("semantic_data")
    sql = "SELECT c_webbnamn_1_sort AS title, ed_tooltip AS content, c_webbfornamn1, c_webbefternamn1 " \
          "FROM persons WHERE id_p=%s"

    with connection.cursor() as cursor:
        cursor.execute(sql, [person_id])
        ms_data = cursor.fetchall()

    connection.close()

    if ms_data and ms_data[0] is not None:
        return jsonify(ms_data[0])
    else:
        return jsonify("")


# routes/semantic_data/persons.php
@digital_edition.route("/semantic_data/persons/list/<data_source_id>")
def get_list_of_persons(data_source_id):
    logger.info("Getting list of persons /semantic_data/persons/list/{}".format(data_source_id))
    open_mysql_connection("semantic_data")
    sql = "SELECT c_webbnamn_1_sort AS title, ed_tooltip AS content, " \
          "c_webbfornamn1, c_webbefternamn1, ed_tooltip, id_p, c_webbsok " \
          "FROM persons WHERE data_source_id=%s ORDER BY id_p ASC"

    with connection.cursor() as cursor:
        cursor.execute(sql, [data_source_id])
        ms_data = cursor.fetchall()

    connection.close()

    return jsonify(ms_data)


# routes/semantic_data/places.php
@digital_edition.route("/semantic_data/places/tooltip/<place_id>")
def get_place_tooltip(place_id):
    logging.info("Getting tooltip /semantic_data/places/tooltip/{}".format(place_id))
    open_mysql_connection("semantic_data")
    place_id = place_id.replace("pl", "").replace("PlId", "")

    sql = "SELECT o_ortnamn AS title, o_beskrivning AS content FROM places WHERE id=%s"
    with connection.cursor() as cursor:
        cursor.execute(sql, [place_id])
        ms_data = cursor.fetchall()

    connection.close()
    return jsonify(ms_data[0])


# routes/semantic_data/places.php
@digital_edition.route("/semantic_data/places/list")
def get_list_of_places():
    logger.info("Getting list of places /semantic_data/places/list")
    open_mysql_connection("semantic_data")

    sql = "SELECT c_webbsok, o_id AS id FROM places ORDER BY o_id ASC"
    with connection.cursor() as cursor:
        cursor.execute(sql)
        ms_data = cursor.fetchall()

    connection.close()
    return jsonify(ms_data)
