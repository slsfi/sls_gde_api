from collections import OrderedDict
from flask import abort, Blueprint, jsonify, request, safe_join
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import pymysql
from ruamel.yaml import YAML
import os
import io

# TODO cache invalidation - check modification time of cache file, if old, discard and regenerate cache

digital_edition = Blueprint('digital_edition', __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as digital_editions_config:
    yaml = YAML()
    project_config = yaml.load(digital_editions_config)

logger = logging.getLogger("sls_api.digital_edition")

file_handler = TimedRotatingFileHandler(filename=project_config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)


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

    with io.open(xml_file_path, encoding="UTF-8") as xml_file:
        xml_contents = xml_file.read()
        if replace_namespace:
            xml_contents = xml_contents.replace('xmlns="http://www.sls.fi/tei"', 'xmlns="http://www.tei-c.org/ns/1.0"')

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


def open_mysql_connection(database):
    class OrderedDictCursor(pymysql.cursors.DictCursorMixin, pymysql.cursors.Cursor):
        dict_type = OrderedDict
    global connection
    if database not in project_config:
        connection = None
    else:
        connection = pymysql.connect(
            host=project_config[database]["address"],
            port=project_config[database]["port"],
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
        with io.open(file_path, encoding="UTF-8") as html_file:
            contents = html_file.read()
        data = {
            "filename": filename,
            "content": contents
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
def get_toc_edition_links(project, edition_id, link_id):
    logger.info("Getting links /{}/table-of-contents/edition/{}/prevnext/{}".format(project, edition_id, link_id))
    return_data = OrderedDict()
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
            if len(toc_data) > 0:
                return_data["prev"] = toc_data[0]
            else:
                return_data["prev"] = None

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
              "WHERE toc_ed_id = %s AND (toc_linkType!=6 OR toc_linkType IS NULL) AND prgroup.group_lansering>={} " \
              "ORDER BY toc_groupid, toc.sortOrder".format(show_published)
    else:
        sql = "SELECT toc.* FROM tableofcontents toc WHERE toc_ed_id=%s AND (toc_linkType!=6 OR toc_linkType IS NULL) " \
              "AND toc.toc_group_id IS NULL ORDER BY toc_groupid, toc.sortOrder"

    with connection.cursor() as cursor:
        cursor.execute(sql, [edition_id])
        toc_data = cursor.fetchall()
    connection.close()

    return_data = OrderedDict()
    sub_group_toc_id = 0
    for iteration in range(1, 3):
        for row in toc_data:
            if iteration == 1:
                if row["toc_groupid"] is None:
                    if row["toc_id"] not in return_data:
                        new_data = {
                                "id": row["toc_id"],
                                "title": row["title"],
                                "titleLevel": row["titleLevel"],
                                "items": []
                            }
                        return_data[int(row["toc_id"])] = new_data
            elif iteration == 2:
                if row["toc_groupid"] is not None and row["toc_linkType"] is None:
                    groupid = row["toc_groupid"]
                    if groupid not in return_data or "items" not in return_data[groupid] or row["toc_id"] not in return_data[groupid]["items"]:
                        # The PHP version builds up these objects, but appears to throw them away or lose them at some point?
                        continue
                        # sub_group_toc_id = row["toc_id"]
                        # new_data = {
                        #     "title": row["title"],
                        #     "id": row["toc_id"],
                        #     "items": []
                        # }
                        # if "items" not in return_data[groupid]:
                        #     return_data[groupid]["items"] = []
                        # return_data[groupid]["items"].insert(row["toc_id"], new_data)
                else:
                    if row["toc_groupid"] is None:
                        continue
                    new_data = {
                        "title": row["title"],
                        "id": row["toc_id"],
                        "link": "{}_{}".format(row["toc_ed_id"], row["toc_linkID"]),
                        "link_type": row["toc_linkType"],
                        "sort_order": row["sortOrder"]
                    }
                    try:
                        return_data[row["toc_groupid"]]["items"][sub_group_toc_id]["items"].insert(row["toc_id"], new_data)
                    except IndexError:
                        return_data[row["toc_groupid"]]["items"].insert(sub_group_toc_id, {"items": [new_data]})

    for key, value in return_data.items():
        # the PHP version seems to drop these at some point
        del value["id"]
        del value["title"]
        del value["titleLevel"]

    return jsonify(list(return_data.values()))


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


@digital_edition.route("/<project>/cache/est/<edition_id>")            # est
@digital_edition.route("/<project>/cache/com/<edition_id>/")           # com
@digital_edition.route("/<project>/cache/inl/<edition_id>/<lang>")     # inl
def check_last_modified(project, edition_id, lang=None):
    """
    Return the modification time for the XML containing the reading text, as seconds since the UNIX epoch
    """
    # TODO in future, check date_modified from database instead
    text_type = request.path.split("/")[4]
    if text_type == "est":
        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(edition_id))
    elif text_type == "com":
        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "com", "{}_com.xml".format(edition_id))
    elif text_type == "inl":
        lang_code = "fin" if lang == "fi" else "swe"
        version = "int" if project_config[project]["show_internally_published"] else "ext"
        filename = "{}_inl_{}_{}.xml".format(edition_id, lang_code, version)

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "inl", filename)
    else:
        return ""
    try:
        return str(os.path.getmtime(xml_file_path)), 200
    except OSError:
        return abort(404)

# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/est/<edition_id>")
def get_publication_est_text(project, edition_id):
    logger.info("Getting XML /{}/text/est/{} and transforming".format(project, edition_id))

    can_show, content = publish_status(project, edition_id)

    if can_show:
        id_parts = edition_id.replace("_est", "").split(";")
        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))

        cache_file_path = safe_join(project_config[project]["file_root"], "cache", "est", "{}_est.html".format(id_parts[0]))

        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))

        if os.path.exists(cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading file from cache."
            else:
                logger.info("Content fetched from cache.")

        elif os.path.exists(xml_file_path):
            logger.warning("No cache found")
            try:
                xsl_file_path = safe_join(project_config["xslt_root"], "est.xsl")
                content = xml_to_html(xsl_file_path, xml_file_path)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
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
@digital_edition.route("/<project>/text/com/<edition_id>")
def get_publication_com_text(project, edition_id, note_id=None):
    logger.info("Getting XML /{}/text/com/{}/{} and transforming".format(project, edition_id, note_id))

    can_show, content = publish_status(project, edition_id)

    if can_show:
        id_parts = edition_id.replace("_com", "").split(";")

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "com", "{}_com.xml".format(id_parts[0]))
        est_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))

        cache_file_path = safe_join(project_config[project]["file_root"], "cache", "com", "note_{}_com_{}.html".format(id_parts[0], note_id))
        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))
        logger.debug("est XML file path is {}".format(est_file_path))

        if os.path.exists(cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        elif os.path.exists(xml_file_path):
            logger.warning("No cache found")
            try:
                params = {
                    "estDocument": '"file://{}"'.format(est_file_path)
                }
                xsl_file = "com.xsl"
                if note_id is not None:
                    params["noteId"] = '"{}"'.format(note_id)
                    xsl_file = "notes.xsl"

                xsl_file_path = safe_join(project_config["xslt_root"], xsl_file)

                content = xml_to_html(xsl_file_path, xml_file_path, params=params)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
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
@digital_edition.route("/<project>/text/ms/<edition_id>")
@digital_edition.route("/<project>/text/ms/<edition_id>/<changes>")
def get_publication_manuscripts(project, edition_id, changes=False):

    can_show, erro_message = publish_status(project, edition_id)

    if can_show:
        item_id, book_id, text_id, section_id = getIdParts(edition_id)
        open_mysql_connection(project)
        manuscript_info = []

        # the content has chapters in the same xml
        sql = "SELECT m_title, m_type, m_filename, m_id FROM manuscripts WHERE m_filename like %s ORDER BY m_sort"
        with connection.cursor() as cursor:
            cursor.execute(sql, [item_id  + "_ms_%"])
            manuscript_info = cursor.fetchall()

        connection.close()

        for i in range(len(manuscript_info)):
            manuscript = manuscript_info[i]
            params = {
                "bookId": book_id
            }
            manuscript_info[i]["manuscript_changes"] = getContent(project, "ms", manuscript["m_filename"], "ms_changes.xsl", params)
            manuscript_info[i]["manuscript_normalized"] = getContent(project, "ms", manuscript["m_filename"], "ms_normalized.xsl", params)

        data = {
            "id": item_id,
            "manuscripts": manuscript_info
        }

    else:
        data = {
            "id": edition_id,
            "error": error_message
        }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/var/<edition_id>")
def get_publication_variations(project, edition_id):
    logger.info("Getting XML /{}/text/var/{} and transforming ...".format(project, edition_id))
    can_show, error_message = publish_status(project, edition_id)

    if can_show:
        item_id, book_id, text_id, section_id = getIdParts(edition_id)
        open_mysql_connection(project)
        variation_info = []

        # the content has chapters in the same xml
        if (section_id is not None):
            sql = "SELECT v_title, v_type, v_filename, v_id FROM versions WHERE v_filename like %s AND v_section_id=%s ORDER BY v_sort"
            with connection.cursor() as cursor:
                cursor.execute(sql, [item_id  + "_var_%", section_id])
                variation_info = cursor.fetchall()
        else:
            sql = "SELECT v_title, v_type, v_filename, v_id FROM versions WHERE v_filename like %s ORDER BY v_sort"
            with connection.cursor() as cursor:
                cursor.execute(sql, [item_id + "_var_%"])
                variation_info = cursor.fetchall()
        connection.close()

        for i in range(len(variation_info)):
            variation = variation_info[i]
            params = {
                "bookId": book_id
            }
            # chapters_xsl_file = "chapters.xsl"

            if variation["v_type"] == "1":
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if (section_id is not None):
                params["sectionId"] = section_id

            variation_info[i] = getContent(project, "var", variation["v_filename"], xsl_file, params)

        data = {
            "id": edition_id,
            "variations": variation_info
        }

    else:
        data = {
            "id": edition_id,
            "error": error_message
        }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
'''
		// -------------------------------------------------
		// Versions
		// -------------------------------------------------
		//Put variants in an array indexed by the title (variant source), be careful to escape ' characters if they exist in text
		$variants = array();
		if($m_sSectionId != '0') {
			$result2 = mysqli_query($db, "SELECT v_title, v_type, v_filename, v_id FROM versions WHERE v_publication_id=".$sPubId." AND v_section_id='".$m_sSectionId."' ORDER BY v_sort");
			$result2 = mysqli_query($db, "SELECT v_title, v_type, v_filename, v_id FROM versions WHERE v_publication_id=".$sPubId." ORDER BY v_sort");
			if(mysqli_num_rows($result2) < 1)
		}
		else
			$result2 = mysqli_query($db, "SELECT v_title, v_type, v_filename, v_id FROM versions WHERE v_publication_id=".$sPubId." ORDER BY v_sort");
		if($result2)
		{
			while ($myrow2 = mysqli_fetch_assoc($result2))
			{
				if(file_exists(GetVarPath().$myrow2['v_filename']))
				{
					if($myrow2['v_type'] == "1")
					{
						if($m_sSectionId != '0')
							$variants[$myrow2['v_title']] = 
                            array("text" => "'".str_replace("'", "&#39;", replaceLineBreaks(
                                XmlFileToHtml(GetVarPath().$myrow2['v_filename'], 
                                'poem_variants_est.xsl', 
                                'sectionId', $m_sSectionId, 
                                'bookId', $m_sBookId)))."'", 
                                "chapters" => XmlFileToHtml (
                                    .$myrow2['v_filename'], 
                                    'chapters.xsl', 
                                    'bookId', $m_sBookId))), 
                                "type" => 1, 
                                "var_id" => $myrow2['v_id']);
						else
							$variants[$myrow2['v_title']] = 
                                array("text" => 
                                    XmlFileToHtml( $myrow2['v_filename'],
                                    $config["folder_xslt"].'
                                    poem_variants_est.xsl', 
                                    'bookId', $m_sBookId)))."'", 
                                    "chapters" => 
                                        trim(replaceLineBreaks(
                                                XmlFileToHtml(
                                                        .$myrow2['v_filename'], 
                                                        'chapters.xsl', 
                                                        'bookId', $m_sBookId)
                                                    )), 
                                    "type" => 1, 
                                    "var_id" => $myrow2['v_id']);
					}
					else
					{
						if($m_sSectionId != '0')
							$variants[$myrow2['v_title']] = array("text" => "'".str_replace("'", "&#39;", replaceLineBreaks(XmlFileToHtml(GetVarPath().$myrow2['v_filename'], $config["folder_xslt"].'poem_variants_other.xsl', 'sectionId', $m_sSectionId)))."'", "chapters" => trim(replaceLineBreaks(XmlFileToHtml(GetVarPath().$myrow2['v_filename'], $config["folder_xslt"].'chapters.xsl', null, null))), "type" => $myrow2['v_type'], "var_id" => $myrow2['v_id']);
						else
							$variants[$myrow2['v_title']] = array("text" => "'".str_replace("'", "&#39;", replaceLineBreaks(XmlFileToHtml(GetVarPath().$myrow2['v_filename'], $config["folder_xslt"].'poem_variants_other.xsl', null, null)))."'", "chapters" => trim(replaceLineBreaks(XmlFileToHtml(GetVarPath().$myrow2['v_filename'], $config["folder_xslt"].'chapters.xsl', null, null))), "type" => $myrow2['v_type'], "var_id" => $myrow2['v_id']);
					}
				}
			}
		}
		if(count($variants) > 0)
		{
			$variantObjects = array();
			foreach ($variants as $title => $variant)
				$variantObjects[] = "{'title' : '$title', 'text' : ".$variant["text"].(strlen($variant["chapters"]) > 4 ? ", 'chapters' : ".$variant["chapters"] : "" ).", 'type' : ".$variant["type"].", 'var_id' : ".$variant["var_id"]."}";
			$variantJS = "[".implode(',', $variantObjects).']';
		}
		else
		{
			$variantJS = "'<div class=\"container cont_comment\"><p class=\"noIndent\">".$phrases['inga_varianter']."</p></div>'";
		}
		// -------------------------------------------------
		// Versions end
		// -------------------------------------------------
'''

# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/inl/<edition_id>")
@digital_edition.route("/<project>/text/inl/<edition_id>/<lang>")
def get_publication_inl_text(project, edition_id, lang=None):
    return get_publication_inl_tit_text(project, edition_id, lang, "inl")

# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/tit/<edition_id>")
@digital_edition.route("/<project>/text/tit/<edition_id>/<lang>")
def get_publication_tit_text(project, edition_id, lang=None):
    return get_publication_inl_tit_text(project, edition_id, lang, "tit")


def get_publication_inl_tit_text(project, edition_id, lang=None, what="inl"):

    if lang is None:
        logger.info("Getting XML /{}/text/{}/{} and transforming".format(project, what, edition_id))
    else:
        logger.info("Getting XML /{}/text/{}/{}/{} and transforming".format(project, what, edition_id, lang))

    can_show, content = publish_status(project, edition_id)

    if can_show:

        lang_code = "fin" if lang == "fi" else "swe"
        version = "int" if project_config[project]["show_internally_published"] else "ext"
        filename = "{}_{}_{}_{}.xml".format(edition_id, what, lang_code, version)

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", what, filename)
        # xml_file_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/xml/inl".format(project), filename))

        cache_file_path = safe_join(project_config[project]["file_root"], "cache", what, filename.replace(".xml", ".html"))
        # cache_folder_path = os.path.realpath(os.path.join(os.path.abspath(__file__), "/../../../../{}-required/cache/inl".format(project)))
        # cache_file_path = os.path.join(cache_folder_path, filename.replace(".xml", ".html"))

        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))

        if os.path.exists(cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                logger.exception("Error reading content from cache")
                content = "Error reading content from cache"
            else:
                logger.info("Content fetched from cache.")
        elif os.path.exists(xml_file_path):
            logger.warning("No cache found")
            try:

                if what == "tit":
                    xsl_file = "title.xsl"
                else:
                    xsl_file = "introduction.xsl"

                xsl_file_path = safe_join(project_config["xslt_root"], xsl_file)
                content = xml_to_html(xsl_file_path, xml_file_path)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
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
        return jsonify("Person not found"), 404


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
    logger.info("Getting tooltip /semantic_data/places/tooltip/{}".format(place_id))
    open_mysql_connection("semantic_data")
    place_id = place_id.replace("pl", "").replace("PlId", "")

    sql = "SELECT o_ortnamn AS title, o_beskrivning AS content FROM places WHERE id=%s"
    with connection.cursor() as cursor:
        cursor.execute(sql, [place_id])
        ms_data = cursor.fetchall()

    connection.close()

    if ms_data and ms_data[0] is not None:
        return jsonify(ms_data[0])
    else:
        return jsonify("Place not found"), 404


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



'''
    HELPER FUNCTIONS  
'''

def publish_status(project, edition_id):
    """Get info on the publications status
    Is is visible to the public etc...

    returns two values:
        - a booelan if the publication can be shown
        - a message text why it can't be shown, if that is the case.
    """
    logger.info("Checking if /{} {} is published".format(project, edition_id))

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

    can_show = False
    content = ""

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
            can_show = True

    return (can_show, content)

def getIdParts(edition_id):
    id_parts = edition_id.replace("_est", "").split(";")    # 12_1_est;ch5  - Finland framställt i teckningar.

    item_id = id_parts[0]    # 1_1
    item_parts = item_id.split("_")
    book_id = item_parts[0]
    text_id = item_parts[1]
    section_id = id_parts[1] if len(id_parts) > 1 else None

    return item_id, book_id, text_id, section_id

def getContent(project, folder, xml_filename, xsl_filename, parameters):
        xml_file_path = safe_join(project_config[project]["file_root"], "xml", folder, xml_filename)
        xsl_file_path = safe_join(project_config["xslt_root"], xsl_filename)
        cache_file_path = xml_file_path.replace("/xml/", "/cache/").replace(".xml", ".html");

        if os.path.exists(cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        elif os.path.exists(xml_file_path):
            logger.warning("No cache found")
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
        else:
            content = "File not found"

        return content
